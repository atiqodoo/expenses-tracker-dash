import dash
from dash import dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import sqlite3
from datetime import datetime
from dash.exceptions import PreventUpdate
import io
import base64
import logging
import time
import socket
import signal
import sys

# Set up logging
logging.basicConfig(
    filename='dash_app.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[
    dbc.themes.BOOTSTRAP,
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css'
])
app.config.suppress_callback_exceptions = True

# === DATABASE SETUP ===
try:
    with sqlite3.connect('expenses.db') as conn:
        logger.info("Database connection established")
        conn.execute('''CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS subcategories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category_id INTEGER,
            FOREIGN KEY (category_id) REFERENCES categories(id),
            UNIQUE(name, category_id))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS wallets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            name TEXT NOT NULL,
            opening_balance REAL NOT NULL,
            current_balance REAL NOT NULL,
            mpesa_number TEXT,
            UNIQUE(name))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, time TEXT, amount REAL,
            wallet_id INTEGER,
            category_id INTEGER,
            subcategory_id INTEGER,
            description TEXT,
            FOREIGN KEY (wallet_id) REFERENCES wallets(id),
            FOREIGN KEY (category_id) REFERENCES categories(id),
            FOREIGN KEY (subcategory_id) REFERENCES subcategories(id))''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date)')
        for cat in ['Food', 'Transport', 'Utilities', 'Entertainment', 'Other']:
            conn.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', (cat,))
        conn.execute('INSERT OR IGNORE INTO wallets (type, name, opening_balance, current_balance) VALUES (?, ?, ?, ?)',
                     ('Cash', 'Main Wallet', 1000.0, 1000.0))
        conn.execute('INSERT OR IGNORE INTO wallets (type, name, opening_balance, current_balance, mpesa_number) VALUES (?, ?, ?, ?, ?)',
                     ('Mpesa', 'Mpesa Wallet', 500.0, 500.0, '1234567890'))
        conn.commit()
    logger.info("Database tables and indices initialized")
except sqlite3.Error as e:
    logger.error(f"Database connection failed: {e}")
    raise

# === HELPER FUNCTIONS ===
wallet_types = ['Cash', 'Mpesa', 'Bank']
hours = [f"{h:02d}" for h in range(24)]
minutes = [f"{m:02d}" for m in range(60)]

def execute_with_retry(query, params=(), retries=3, delay=1):
    """Execute SQLite query with retry on database lock, using a new connection."""
    for attempt in range(retries):
        conn = sqlite3.connect('expenses.db')
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchall() if query.strip().upper().startswith('SELECT') else None
            conn.commit()
            return result
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < retries - 1:
                logger.warning(f"Database locked, retrying {attempt + 1}/{retries}")
                time.sleep(delay)
                continue
            logger.error(f"execute_with_retry failed: {e}")
            raise
        except sqlite3.Error as e:
            logger.error(f"execute_with_retry failed: {e}")
            raise
        finally:
            conn.close()

def get_categories():
    try:
        rows = execute_with_retry('SELECT id, name FROM categories')
        df = pd.DataFrame(rows, columns=['id', 'name']) if rows else pd.DataFrame(columns=['id', 'name'])
        if df.empty:
            logger.warning("get_categories: No categories found")
        return df
    except sqlite3.Error as e:
        logger.error(f"get_categories failed: {e}")
        return pd.DataFrame(columns=['id', 'name'])

def get_subcategories(category_id=None):
    try:
        if category_id:
            rows = execute_with_retry('SELECT id, name FROM subcategories WHERE category_id = ?', (category_id,))
        else:
            rows = execute_with_retry('SELECT id, name FROM subcategories')
        df = pd.DataFrame(rows, columns=['id', 'name']) if rows else pd.DataFrame(columns=['id', 'name'])
        if df.empty:
            logger.info(f"get_subcategories: No subcategories found for category_id={category_id}")
        return df
    except sqlite3.Error as e:
        logger.error(f"get_subcategories failed: {e}")
        return pd.DataFrame(columns=['id', 'name'])

def get_wallets():
    try:
        rows = execute_with_retry('SELECT id, name, type, current_balance, mpesa_number FROM wallets')
        df = pd.DataFrame(rows, columns=['id', 'name', 'type', 'current_balance', 'mpesa_number']) if rows else pd.DataFrame(columns=['id', 'name', 'type', 'current_balance', 'mpesa_number'])
        if df.empty:
            logger.warning("get_wallets: No wallets found")
        return df
    except sqlite3.Error as e:
        logger.error(f"get_wallets failed: {e}")
        return pd.DataFrame(columns=['id', 'name', 'type', 'current_balance', 'mpesa_number'])

def is_wallet_unused(wallet_id):
    try:
        rows = execute_with_retry('SELECT COUNT(*) FROM expenses WHERE wallet_id = ?', (wallet_id,))
        count = rows[0][0] if rows else 0
        return count == 0
    except sqlite3.Error as e:
        logger.error(f"is_wallet_unused failed: {e}")
        return False

def get_expenses():
    try:
        rows = execute_with_retry('''
            SELECT e.id, e.date, e.time, e.amount,
                   w.name as wallet, c.name as category, 
                   s.name as subcategory, e.description
            FROM expenses e
            LEFT JOIN wallets w ON e.wallet_id = w.id
            LEFT JOIN categories c ON e.category_id = c.id
            LEFT JOIN subcategories s ON e.subcategory_id = s.id
            ORDER BY e.date DESC, e.time DESC
            LIMIT 1000
        ''')
        if not rows:
            logger.info("get_expenses: No expenses found")
            return pd.DataFrame(columns=['ID', 'Date', 'Time', 'Amount', 'Wallet', 'Category', 'Subcategory', 'Description', 'Delete'])
        df = pd.DataFrame(rows, columns=[
            'ID', 'Date', 'Time', 'Amount', 'Wallet', 'Category', 'Subcategory', 'Description'
        ])
        df['Delete'] = '<i class="fas fa-trash"></i>'
        return df
    except sqlite3.Error as e:
        logger.error(f"get_expenses failed: {e}")
        return pd.DataFrame(columns=['ID', 'Date', 'Time', 'Amount', 'Wallet', 'Category', 'Subcategory', 'Description', 'Delete'])

def get_wallets_for_table():
    try:
        df = get_wallets()
        if df.empty:
            return pd.DataFrame(columns=['ID', 'Name', 'Type', 'Balance', 'Mpesa Number', 'Delete'])
        df = df.rename(columns={
            'id': 'ID',
            'name': 'Name',
            'type': 'Type',
            'current_balance': 'Balance',
            'mpesa_number': 'Mpesa Number'
        })
        df['Balance'] = df['Balance'].apply(lambda x: f"KES {x:.2f}")
        df['Mpesa Number'] = df['Mpesa Number'].fillna('-')
        df['Delete'] = '<i class="fas fa-trash"></i>'
        return df
    except Exception as e:
        logger.error(f"get_wallets_for_table failed: {e}")
        return pd.DataFrame(columns=['ID', 'Name', 'Type', 'Balance', 'Mpesa Number', 'Delete'])

# === APP LAYOUT ===
app.layout = dbc.Container([
    dcc.Store(id='app-load'),
    dcc.Store(id='expenses-data'),
    dbc.Row([
        dbc.Col([
            html.H4("Manage Categories", className="my-3"),
            dbc.Input(id='new-category-input', placeholder='New Category', className='mb-2'),
            dbc.Button('Add Category', id='add-category-btn', color='secondary', className='mb-3'),
            dcc.Dropdown(id='parent-category-dropdown', placeholder='Parent Category', className='mb-2'),
            dbc.Input(id='new-subcategory-input', placeholder='New Subcategory', className='mb-2'),
            dbc.Button('Add Subcategory', id='add-subcategory-btn', color='secondary', className='mb-3'),
            html.Hr(),
            html.H4("Manage Wallets", className="my-3"),
            dbc.Input(id='wallet-name-input', placeholder='Wallet Name', className='mb-2'),
            dcc.Dropdown(id='wallet-type-input', options=[{'label': t, 'value': t} for t in wallet_types], 
                        placeholder='Type', className='mb-2'),
            dbc.Input(id='opening-balance-input', type='number', placeholder='Opening Balance', min=0, className='mb-2'),
            dbc.Input(id='mpesa-number-input', placeholder='Mpesa Number (optional)', className='mb-2'),
            dbc.Button('Add Wallet', id='add-wallet-btn', color='secondary', className='mb-4'),
            html.Hr(),
            html.H5("Wallets List", className="my-3"),
            dash_table.DataTable(
                id='wallets-table',
                columns=[
                    {"name": i, "id": i} for i in ['Name', 'Type', 'Balance', 'Mpesa Number']
                ] + [{"name": "Delete", "id": "Delete", "presentation": "markdown"}],
                data=[],
                style_table={'overflowX': 'auto'},
                style_cell={'textAlign': 'left'},
                style_data_conditional=[
                    {
                        'if': {'column_id': 'Delete'},
                        'textAlign': 'center',
                        'color': 'red',
                        'cursor': 'pointer'
                    }
                ],
                page_size=5
            )
        ], width=3),

        dbc.Col([
            html.H2("Expense Tracker", className="text-center my-4"),
            dbc.Button("Add Expense", id="add-expense-button", color="primary"),
            dbc.Modal([
                dbc.ModalHeader("Add Expense"),
                dbc.ModalBody([
                    dbc.Row([
                        dbc.Col([dbc.Label("Date"), dcc.DatePickerSingle(
                            id='date-input', 
                            date=datetime.today(),
                            display_format='YYYY-MM-DD'
                        )]),
                        dbc.Col([dbc.Label("Time"), dbc.Row([
                            dbc.Col(dcc.Dropdown(
                                id='hour-input', 
                                options=[{'label': h, 'value': h} for h in hours],
                                value='12'
                            )),
                            dbc.Col(dcc.Dropdown(
                                id='minute-input', 
                                options=[{'label': m, 'value': m} for m in minutes],
                                value='00'
                            ))
                        ])])
                    ]),
                    dbc.Row([
                        dbc.Col([dbc.Label("Amount"), dbc.Input(
                            id='amount-input', 
                            type='number',
                            min=0,
                            step=0.01
                        )]),
                        dbc.Col([dbc.Label("Wallet"), dcc.Dropdown(id='wallet-input')])
                    ]),
                    dbc.Row([
                        dbc.Col([dbc.Label("Category"), dcc.Dropdown(id='category-input')]),
                        dbc.Col([dbc.Label("Subcategory (optional)"), dcc.Dropdown(id='subcategory-input')])
                    ]),
                    dbc.Row([
                        dbc.Col([dbc.Label("Description"), dbc.Input(id='description-input', type='text')])
                    ])
                ]),
                dbc.ModalFooter([
                    dbc.Button("Close", id="close-button"),
                    dbc.Button("Save", id="save-button", color="primary")
                ])
            ], id="expense-modal", is_open=False),

            dash_table.DataTable(
                id='expenses-table',
                columns=[
                    {"name": i, "id": i} for i in ['Date', 'Time', 'Amount', 'Wallet', 'Category', 'Subcategory', 'Description']
                ] + [{"name": "Delete", "id": "Delete", "presentation": "markdown"}],
                data=[],
                style_table={'overflowX': 'auto'},
                style_cell={'textAlign': 'left'},
                style_data_conditional=[
                    {
                        'if': {'column_id': 'Delete'},
                        'textAlign': 'center',
                        'color': 'red',
                        'cursor': 'pointer'
                    }
                ],
                page_size=10,
                filter_action='native',
                sort_action='native'
            ),

            html.Hr(),
            html.H4("Filters and Export", className="my-4"),
            dbc.Row([
                dbc.Col(dcc.DatePickerRange(
                    id='date-range-filter',
                    display_format='YYYY-MM-DD',
                    start_date=datetime.today().replace(day=1),
                    end_date=datetime.today(),
                    className='mb-2'),
                width=4),
                dbc.Col(dcc.Dropdown(
                    id='filter-category-input', 
                    placeholder='Filter by Category',
                    multi=True,
                    className='mb-2'), 
                width=4),
                dbc.Col(dbc.Button(
                    "Export to Excel", 
                    id='export-excel-btn', 
                    color='success',
                    href="",
                    download="expenses.xlsx"),
                width=4)
            ]),
            html.H4("Dashboard Charts", className="my-4"),
            dbc.Row([
                dbc.Col(dcc.Graph(id='category-pie-chart'), width=6),
                dbc.Col(dcc.Graph(id='monthly-bar-chart'), width=6)
            ])
        ], width=9)
    ]),
    
    # Toast notifications
    dbc.Toast(
        "Expense deleted successfully.", 
        id="delete-toast", 
        header="Deleted", 
        icon="danger", 
        duration=3000, 
        dismissable=True, 
        is_open=False, 
        style={"position": "fixed", "top": 10, "right": 10, "zIndex": 9999}
    ),
    dbc.Toast(
        "Expense added successfully.", 
        id="add-toast", 
        header="Success", 
        icon="success", 
        duration=3000, 
        dismissable=True, 
        is_open=False, 
        style={"position": "fixed", "top": 10, "right": 10, "zIndex": 9999}
    ),
    dbc.Toast(
        "Invalid input or insufficient wallet balance. Please check your data.",
        id="error-toast",
        header="Error",
        icon="danger",
        duration=3000,
        dismissable=True,
        is_open=False,
        style={"position": "fixed", "top": 10, "right": 10, "zIndex": 9999}
    ),
    dbc.Toast(
        "",  # Dynamic message set in callback
        id="balance-toast",
        header="Wallet Balance",
        icon="info",
        duration=3000,
        dismissable=True,
        is_open=False,
        style={"position": "fixed", "top": 10, "right": 10, "zIndex": 9999}
    ),
    dbc.Toast(
        "",  # Dynamic message set in callback
        id="delete-wallet-toast",
        header="Wallet Deletion",
        icon="info",
        duration=3000,
        dismissable=True,
        is_open=False,
        style={"position": "fixed", "top": 10, "right": 10, "zIndex": 9999}
    )
], fluid=True)

# === CALLBACKS ===

# Initialize dropdowns and expenses table
@app.callback(
    [Output('category-input', 'options'),
     Output('parent-category-dropdown', 'options'),
     Output('filter-category-input', 'options'),
     Output('wallet-input', 'options'),
     Output('expenses-table', 'data'),
     Output('wallets-table', 'data')],
    Input('app-load', 'data')
)
def initialize_dropdowns_and_table(_):
    logger.info("initialize_dropdowns_and_table triggered")
    try:
        categories_df = get_categories()
        wallets_df = get_wallets()
        expenses_df = get_expenses()
        wallets_table_df = get_wallets_for_table()
        logger.debug(f"Categories: {categories_df.to_dict()}")
        logger.debug(f"Wallets: {wallets_df.to_dict()}")
        logger.debug(f"Expenses: {expenses_df.to_dict()}")
        logger.debug(f"Wallets Table: {wallets_table_df.to_dict()}")
        
        if categories_df.empty:
            logger.warning("No categories found")
        if wallets_df.empty:
            logger.warning("No wallets found")
        if expenses_df.empty:
            logger.warning("No expenses found")
        if wallets_table_df.empty:
            logger.warning("No wallets found for table")
        
        categories = [{'label': row['name'], 'value': row['id']} 
                     for _, row in categories_df.iterrows()]
        wallets = [{'label': f"{row['name']} (KES {row['current_balance']:.2f})", 'value': row['id']} 
                  for _, row in wallets_df.iterrows()]
        expenses = expenses_df.to_dict('records')
        wallets_table = wallets_table_df.to_dict('records')
        logger.info("initialize_dropdowns_and_table completed")
        return categories, categories, categories, wallets, expenses, wallets_table
    except Exception as e:
        logger.error(f"initialize_dropdowns_and_table failed: {e}")
        return [], [], [], [], [], []

# Add category
@app.callback(
    [Output('new-category-input', 'value'),
     Output('category-input', 'options', allow_duplicate=True),
     Output('parent-category-dropdown', 'options', allow_duplicate=True),
     Output('filter-category-input', 'options', allow_duplicate=True),
     Output('error-toast', 'is_open')],
    Input('add-category-btn', 'n_clicks'),
    State('new-category-input', 'value'),
    prevent_initial_call=True
)
def add_category(n_clicks, name):
    logger.info("add_category triggered")
    try:
        if not name or not isinstance(name, str) or name.strip() == "":
            logger.warning("add_category: Invalid or empty category name")
            return "", dash.no_update, dash.no_update, dash.no_update, True
        execute_with_retry('INSERT OR IGNORE INTO categories (name) VALUES (?)', (name.strip(),))
        categories = [{'label': row['name'], 'value': row['id']} 
                     for _, row in get_categories().iterrows()]
        logger.info("add_category completed")
        return "", categories, categories, categories, False
    except sqlite3.IntegrityError:
        logger.warning("add_category: Duplicate category name")
        return "", dash.no_update, dash.no_update, dash.no_update, True
    except Exception as e:
        logger.error(f"add_category failed: {e}")
        return "", dash.no_update, dash.no_update, dash.no_update, True

# Load subcategories when category changes
@app.callback(
    Output('subcategory-input', 'options'),
    Input('category-input', 'value'),
    prevent_initial_call=True
)
def load_subcategories(category_id):
    logger.info(f"load_subcategories triggered with category_id: {category_id}")
    try:
        if not category_id:
            logger.debug("load_subcategories: No category selected, returning default")
            return [{'label': 'None', 'value': ''}]
        subcategories = [{'label': 'None', 'value': ''}] + [
            {'label': row['name'], 'value': row['id']} 
            for _, row in get_subcategories(category_id).iterrows()
        ]
        logger.info(f"load_subcategories completed with {len(subcategories)} options")
        return subcategories
    except Exception as e:
        logger.error(f"load_subcategories failed: {e}")
        return [{'label': 'None', 'value': ''}]

# Add subcategory
@app.callback(
    [Output('new-subcategory-input', 'value'),
     Output('subcategory-input', 'options', allow_duplicate=True),
     Output('error-toast', 'is_open', allow_duplicate=True)],
    Input('add-subcategory-btn', 'n_clicks'),
    [State('parent-category-dropdown', 'value'),
     State('new-subcategory-input', 'value')],
    prevent_initial_call=True
)
def add_subcategory(n_clicks, category_id, name):
    logger.info("add_subcategory triggered")
    try:
        if not all([category_id, name]) or not isinstance(name, str) or name.strip() == "":
            logger.warning("add_subcategory: Missing or invalid category_id or name")
            return "", dash.no_update, True
        execute_with_retry('INSERT OR IGNORE INTO subcategories (name, category_id) VALUES (?, ?)', 
                          (name.strip(), category_id))
        subcategories = [{'label': 'None', 'value': ''}] + [
            {'label': row['name'], 'value': row['id']} 
            for _, row in get_subcategories(category_id).iterrows()
        ]
        logger.info("add_subcategory completed")
        return "", subcategories, False
    except sqlite3.IntegrityError:
        logger.warning("add_subcategory: Duplicate subcategory name")
        return "", dash.no_update, True
    except Exception as e:
        logger.error(f"add_subcategory failed: {e}")
        return "", dash.no_update, True

# Add wallet
@app.callback(
    [Output('wallet-name-input', 'value'),
     Output('wallet-type-input', 'value'),
     Output('opening-balance-input', 'value'),
     Output('mpesa-number-input', 'value'),
     Output('wallet-input', 'options', allow_duplicate=True),
     Output('error-toast', 'is_open', allow_duplicate=True),
     Output('wallets-table', 'data', allow_duplicate=True)],
    Input('add-wallet-btn', 'n_clicks'),
    [State('wallet-name-input', 'value'),
     State('wallet-type-input', 'value'),
     State('opening-balance-input', 'value'),
     State('mpesa-number-input', 'value')],
    prevent_initial_call=True
)
def add_wallet(n_clicks, name, wtype, opening, mpesa):
    logger.info("add_wallet triggered")
    try:
        if not all([name, wtype, opening is not None]) or opening < 0 or not isinstance(name, str):
            logger.warning("add_wallet: Invalid input")
            return "", None, None, None, dash.no_update, True, dash.no_update
        execute_with_retry('''INSERT INTO wallets 
                             (type, name, opening_balance, current_balance, mpesa_number) 
                             VALUES (?, ?, ?, ?, ?)''',
                          (wtype, name.strip(), opening, opening, mpesa))
        wallets = [{'label': f"{row['name']} (KES {row['current_balance']:.2f})", 'value': row['id']} 
                  for _, row in get_wallets().iterrows()]
        wallets_table = get_wallets_for_table().to_dict('records')
        logger.info("add_wallet completed")
        return "", None, None, None, wallets, False, wallets_table
    except sqlite3.IntegrityError:
        logger.warning("add_wallet: Duplicate wallet name")
        return "", None, None, None, dash.no_update, True, dash.no_update
    except Exception as e:
        logger.error(f"add_wallet failed: {e}")
        return "", None, None, None, dash.no_update, True, dash.no_update

# Delete wallet
@app.callback(
    [Output('wallets-table', 'data', allow_duplicate=True),
     Output('wallet-input', 'options', allow_duplicate=True),
     Output('delete-wallet-toast', 'is_open'),
     Output('delete-wallet-toast', 'children'),
     Output('delete-wallet-toast', 'icon')],
    Input('wallets-table', 'active_cell'),
    State('wallets-table', 'data'),
    prevent_initial_call=True
)
def delete_wallet(active_cell, data):
    logger.info(f"delete_wallet triggered with active_cell: {active_cell}")
    try:
        if not active_cell or not data or active_cell['column_id'] != 'Delete':
            logger.warning("delete_wallet: Invalid active_cell or data")
            raise PreventUpdate
        row_idx = active_cell['row']
        if row_idx >= len(data):
            logger.warning(f"delete_wallet: Invalid row index {row_idx}")
            raise PreventUpdate
        row = data[row_idx]
        wallet_id = row.get('ID')
        wallet_name = row.get('Name')
        if not wallet_id:
            logger.warning("delete_wallet: No ID in row")
            raise PreventUpdate
        
        # Check if wallet is unused
        if not is_wallet_unused(wallet_id):
            logger.info(f"delete_wallet: Wallet {wallet_id} ({wallet_name}) has expenses, cannot delete")
            return dash.no_update, dash.no_update, True, f"Cannot delete {wallet_name}: It has associated expenses.", "danger"
        
        # Delete wallet
        execute_with_retry('DELETE FROM wallets WHERE id = ?', (wallet_id,))
        
        # Update wallets table and dropdown
        wallets_table = get_wallets_for_table().to_dict('records')
        wallets = [{'label': f"{row['name']} (KES {row['current_balance']:.2f})", 'value': row['id']} 
                  for _, row in get_wallets().iterrows()]
        
        logger.info("delete_wallet completed")
        return wallets_table, wallets, True, f"Wallet {wallet_name} deleted successfully.", "success"
    except sqlite3.Error as e:
        logger.error(f"delete_wallet failed: {e}")
        return dash.no_update, dash.no_update, True, "Error deleting wallet.", "danger"
    except Exception as e:
        logger.error(f"delete_wallet failed: {e}")
        return dash.no_update, dash.no_update, True, "Error deleting wallet.", "danger"

# Toggle expense modal
@app.callback(
    Output('expense-modal', 'is_open'),
    [Input('add-expense-button', 'n_clicks'),
     Input('close-button', 'n_clicks'),
     Input('save-button', 'n_clicks')],
    State('expense-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_modal(add_clicks, close_clicks, save_clicks, is_open):
    logger.info("toggle_modal triggered")
    try:
        ctx = dash.callback_context
        if not ctx.triggered:
            logger.warning("toggle_modal: No triggered context")
            raise PreventUpdate
        prop_id = ctx.triggered[0]['prop_id']
        if prop_id in ['add-expense-button.n_clicks', 'close-button.n_clicks']:
            logger.info("toggle_modal: Toggling modal state")
            return not is_open
        logger.info("toggle_modal: Closing modal after save")
        return False
    except Exception as e:
        logger.error(f"toggle_modal failed: {e}")
        raise PreventUpdate

# Save expense
@app.callback(
    [Output('expenses-table', 'data', allow_duplicate=True),
     Output('add-toast', 'is_open'),
     Output('amount-input', 'value'),
     Output('description-input', 'value'),
     Output('error-toast', 'is_open', allow_duplicate=True),
     Output('wallet-input', 'options', allow_duplicate=True),
     Output('balance-toast', 'is_open'),
     Output('balance-toast', 'children')],
    Input('save-button', 'n_clicks'),
    [State('date-input', 'date'),
     State('hour-input', 'value'),
     State('minute-input', 'value'),
     State('amount-input', 'value'),
     State('wallet-input', 'value'),
     State('category-input', 'value'),
     State('subcategory-input', 'value'),
     State('description-input', 'value')],
    prevent_initial_call=True
)
def save_expense(n_clicks, date, hour, minute, amount, wallet_id, category_id, subcategory_id, desc):
    logger.info(f"save_expense triggered with inputs: date={date}, amount={amount}, wallet_id={wallet_id}, subcategory_id={subcategory_id}")
    try:
        if not all([date, hour, minute, amount is not None, wallet_id, category_id]) or amount <= 0:
            logger.warning("save_expense: Invalid or missing input")
            return dash.no_update, False, None, None, True, dash.no_update, False, ""
        
        # Check wallet balance
        rows = execute_with_retry('SELECT name, current_balance FROM wallets WHERE id = ?', (wallet_id,))
        if not rows:
            logger.warning(f"save_expense: Wallet ID {wallet_id} not found")
            return dash.no_update, False, None, None, True, dash.no_update, False, ""
        wallet_name, balance = rows[0]
        if balance < amount:
            logger.warning(f"save_expense: Insufficient wallet balance for wallet_id {wallet_id}, balance={balance}")
            return dash.no_update, False, None, None, True, dash.no_update, True, f"Insufficient balance in {wallet_name}: KES {balance:.2f}"
        
        # Save expense and update balance
        time_str = f"{hour}:{minute}"
        db_subcategory_id = None if subcategory_id == '' else subcategory_id
        execute_with_retry('''INSERT INTO expenses 
                             (date, time, amount, wallet_id, category_id, subcategory_id, description) 
                             VALUES (?, ?, ?, ?, ?, ?, ?)''',
                          (date, time_str, amount, wallet_id, category_id, db_subcategory_id, desc))
        execute_with_retry('UPDATE wallets SET current_balance = current_balance - ? WHERE id = ?', 
                          (amount, wallet_id))
        
        # Fetch updated expenses and wallets
        df = get_expenses()
        wallets_df = get_wallets()
        wallets = [{'label': f"{row['name']} (KES {row['current_balance']:.2f})", 'value': row['id']} 
                  for _, row in wallets_df.iterrows()]
        
        # Prepare balance toast message
        new_balance = balance - amount
        balance_message = f"Expense added. New balance for {wallet_name}: KES {new_balance:.2f}"
        
        logger.info("save_expense completed")
        return df.to_dict('records'), True, None, None, False, wallets, True, balance_message
    except sqlite3.Error as e:
        logger.error(f"save_expense failed: {e}")
        return dash.no_update, False, None, None, True, dash.no_update, False, ""
    except Exception as e:
        logger.error(f"save_expense failed: {e}")
        return dash.no_update, False, None, None, True, dash.no_update, False, ""

# Delete expense
@app.callback(
    [Output('expenses-table', 'data', allow_duplicate=True),
     Output('delete-toast', 'is_open'),
     Output('wallet-input', 'options', allow_duplicate=True)],
    Input('expenses-table', 'active_cell'),
    State('expenses-table', 'data'),
    prevent_initial_call=True
)
def delete_expense(active_cell, data):
    logger.info(f"delete_expense triggered with active_cell: {active_cell}")
    try:
        if not active_cell or not data or active_cell['column_id'] != 'Delete':
            logger.warning("delete_expense: Invalid active_cell or data")
            raise PreventUpdate
        row_idx = active_cell['row']
        if row_idx >= len(data):
            logger.warning(f"delete_expense: Invalid row index {row_idx}")
            raise PreventUpdate
        row = data[row_idx]
        expense_id = row.get('ID')
        if not expense_id:
            logger.warning("delete_expense: No ID in row")
            raise PreventUpdate
        
        # Get expense details to restore wallet balance
        rows = execute_with_retry('SELECT wallet_id, amount FROM expenses WHERE id = ?', (expense_id,))
        if not rows:
            logger.warning(f"delete_expense: Expense ID {expense_id} not found")
            raise PreventUpdate
        wallet_id, amount = rows[0]
        
        # Delete expense and restore wallet balance
        execute_with_retry('DELETE FROM expenses WHERE id = ?', (expense_id,))
        execute_with_retry('UPDATE wallets SET current_balance = current_balance + ? WHERE id = ?', 
                          (amount, wallet_id))
        
        # Fetch updated expenses and wallets
        df = get_expenses()
        wallets_df = get_wallets()
        wallets = [{'label': f"{row['name']} (KES {row['current_balance']:.2f})", 'value': row['id']} 
                  for _, row in wallets_df.iterrows()]
        
        logger.info("delete_expense completed")
        return df.to_dict('records'), True, wallets
    except sqlite3.Error as e:
        logger.error(f"delete_expense failed: {e}")
        raise PreventUpdate
    except Exception as e:
        logger.error(f"delete_expense failed: {e}")
        raise PreventUpdate

# Update charts
@app.callback(
    [Output('category-pie-chart', 'figure'),
     Output('monthly-bar-chart', 'figure')],
    [Input('expenses-table', 'data'),
     Input('filter-category-input', 'value'),
     Input('date-range-filter', 'start_date'),
     Input('date-range-filter', 'end_date')],
    prevent_initial_call=True
)
def update_charts(data, selected_cats, start_date, end_date):
    logger.info("update_charts triggered")
    try:
        if not data:
            logger.warning("update_charts: No data provided")
            return px.pie(), px.bar()
        df = pd.DataFrame(data)
        
        # Filter by date range
        if start_date and end_date:
            df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        
        # Filter by selected categories
        if selected_cats and len(selected_cats) > 0:
            df = df[df['Category'].isin(selected_cats)]
        
        # Create pie chart
        pie_data = df.groupby('Category')['Amount'].sum().reset_index()
        pie_fig = px.pie(
            pie_data,
            names='Category', 
            values='Amount', 
            title='Spending by Category'
        ) if not pie_data.empty else px.pie()
        
        # Create monthly bar chart
        df = df[df['Date'].notnull()]  # Remove null dates
        df['Month'] = pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce').dt.to_period('M').astype(str)
        df = df[df['Month'].notnull()]  # Remove invalid dates
        monthly_data = df.groupby('Month')['Amount'].sum().reset_index()
        bar_fig = px.bar(
            monthly_data,
            x='Month', 
            y='Amount', 
            title='Monthly Expenses',
            labels={'Amount': 'Total Amount (KES)'}
        ) if not monthly_data.empty else px.bar()
        
        logger.info("update_charts completed")
        return pie_fig, bar_fig
    except Exception as e:
        logger.error(f"update_charts failed: {e}")
        return px.pie(), px.bar()

# Export to Excel
@app.callback(
    Output('export-excel-btn', 'href'),
    Input('export-excel-btn', 'n_clicks'),
    [State('expenses-table', 'data'),
     State('filter-category-input', 'value'),
     State('date-range-filter', 'start_date'),
     State('date-range-filter', 'end_date')],
    prevent_initial_call=True
)
def export_to_excel(n_clicks, data, selected_cats, start_date, end_date):
    logger.info("export_to_excel triggered")
    try:
        if n_clicks is None or not data:
            logger.warning("export_to_excel: No clicks or data")
            raise PreventUpdate
        
        df = pd.DataFrame(data).drop(columns=['ID', 'Delete'], errors='ignore')
        
        # Apply the same filters as the charts
        if start_date and end_date:
            df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        if selected_cats and len(selected_cats) > 0:
            df = df[df['Category'].isin(selected_cats)]
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Expenses', index=False)
        
        output.seek(0)
        encoded = base64.b64encode(output.read()).decode()
        logger.info("export_to_excel completed")
        return f"data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{encoded}"
    except Exception as e:
        logger.error(f"export_to_excel failed: {e}")
        raise PreventUpdate

# Check if port is in use
def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return False
        except socket.error:
            return True

# Graceful shutdown
def signal_handler(sig, frame):
    logger.info("Shutting down Dash app")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Run the app
if __name__ == '__main__':
    port = 8050
    max_attempts = 10
    for i in range(max_attempts):
        if is_port_in_use(port):
            logger.warning(f"Port {port} is in use, trying {port + 1}")
            port += 1
        else:
            logger.info(f"Starting Dash app on port {port}")
            print(f"Dash is running on http://127.0.0.1:{port}/")
            app.run(debug=True, port=port)
            break
    else:
        logger.error(f"Could not find free port after {max_attempts} attempts")
        print(f"Error: Could not find free port. Please stop other instances.")
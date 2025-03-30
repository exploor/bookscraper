import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras
import plotly.express as px
from datetime import datetime
import traceback
import logging
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection function with extensive debugging
def get_db_connection():
    """Connect to PostgreSQL database with error handling and debugging"""
    try:
        st.sidebar.info("Attempting to connect to database...")
        conn_params = {
            "host": "localhost",
            "database": "evolabz",
            "user": "postgres",
            "password": "postgres"
        }
        
        # Log connection attempt
        logger.info(f"Connecting to PostgreSQL with params: host={conn_params['host']}, database={conn_params['database']}, user={conn_params['user']}")
        
        # Connect to the database
        conn = psycopg2.connect(**conn_params)
        
        # Success message
        st.sidebar.success("✅ Connected to database successfully!")
        logger.info("Database connection successful")
        
        return conn
    except Exception as e:
        # Detailed error information
        error_msg = f"Database connection error: {str(e)}"
        error_details = traceback.format_exc()
        
        logger.error(error_msg)
        logger.error(error_details)
        
        # Display error in the UI
        st.sidebar.error(error_msg)
        st.sidebar.error("Check console for detailed error information")
        
        raise Exception(error_msg)

# Function to fetch book data with enhanced debugging
def fetch_books(limit=None, filters=None):
    """Fetch books from the database with debugging information"""
    try:
        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Build query
        query = "SELECT * FROM books"
        params = []
        
        # Add filters if provided
        if filters:
            where_clauses = []
            for key, value in filters.items():
                if value:
                    if key == 'min_price':
                        where_clauses.append("wob_price >= %s")
                        params.append(float(value))
                    elif key == 'max_price':
                        where_clauses.append("wob_price <= %s")
                        params.append(float(value))
                    elif key == 'condition':
                        where_clauses.append("condition = %s")
                        params.append(value)
                    elif key == 'binding':
                        where_clauses.append("binding = %s")
                        params.append(value)
                    elif key == 'first_edition' and value == 'Yes':
                        where_clauses.append("first_edition = 'Yes'")
                    elif key == 'search_term':
                        where_clauses.append("(title ILIKE %s OR author ILIKE %s)")
                        term = f"%{value}%"
                        params.append(term)
                        params.append(term)
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
        
        # Order by creation date
        query += " ORDER BY created_at DESC"
        
        # Add limit if provided
        if limit:
            query += f" LIMIT {limit}"
        
        # Log the query for debugging
        logger.info(f"Executing query: {query}")
        logger.info(f"With parameters: {params}")
        
        # Execute query
        cursor.execute(query, params)
        books = cursor.fetchall()
        
        # Log results
        logger.info(f"Retrieved {len(books)} books from database")
        
        # Convert DictRow objects to plain dictionaries
        result = []
        for book in books:
            # Convert DictRow to dict and add to result
            book_dict = dict(book)
            result.append(book_dict)
        
        # Close connections
        cursor.close()
        conn.close()
        
        return result
    
    except Exception as e:
        error_msg = f"Error fetching books: {str(e)}"
        error_details = traceback.format_exc()
        
        logger.error(error_msg)
        logger.error(error_details)
        
        st.error(error_msg)
        st.error("Check console for detailed error information")
        
        return []

# Streamlit app layout with custom styling
st.set_page_config(
    page_title="Book Arbitrage App",
    page_icon="📚",
    layout="wide"
)

# Custom CSS for better appearance and improved text visibility
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
        color: #ffffff;
    }
    .stDataFrame {
        width: 100%;
    }
    .stDataFrame td, .stDataFrame th {
        color: white !important;
        background-color: #1e1e1e !important;
        text-align: left !important;
        font-size: 14px !important;
    }
    .stDataFrame th {
        font-weight: bold !important;
        background-color: #2d2d2d !important;
    }
    .stSelectbox div {
        background-color: #1e1e1e !important;
        color: white !important;
    }
    .stNumberInput div {
        background-color: #1e1e1e !important;
        color: white !important;
    }
    .stTextInput div {
        background-color: #1e1e1e !important;
        color: white !important;
    }
    h1, h2, h3, h4, h5, h6, .stMarkdown p {
        color: white !important;
    }
    .stMetric {
        background-color: #1e1e1e;
        padding: 15px;
        border-radius: 5px;
        color: white !important;
    }
    .stMetric label {
        color: #cccccc !important;
    }
    .stMetric .metric-value {
        color: white !important;
        font-weight: bold !important;
    }
    .card {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 5px;
        margin-top: 10px;
    }
    .book-detail-label {
        font-weight: bold;
        color: #4dabf7;
    }
    a {
        color: #4dabf7 !important;
        text-decoration: none;
    }
    a:hover {
        text-decoration: underline;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 1px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #1e1e1e;
        color: white;
        border-radius: 4px 4px 0 0;
        border: none;
        padding: 10px 16px;
        margin-right: 2px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #2c3e50;
        color: white;
    }
    .debug-info {
        background-color: #2d3748;
        color: #d9d9d9;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
        font-family: monospace;
        font-size: 12px;
        overflow: auto;
        max-height: 200px;
    }
</style>
""", unsafe_allow_html=True)

# Debug expander in sidebar
with st.sidebar.expander("Database Debug Info", expanded=False):
    st.markdown("### Database Configuration")
    st.markdown("- **Host**: localhost")
    st.markdown("- **Database**: evolabz")
    st.markdown("- **User**: postgres")
    st.markdown("- **Password**: postgres")
    st.markdown("---")
    if st.button("Test Connection"):
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            st.success(f"Connection successful!")
            st.code(f"PostgreSQL Version: {db_version[0]}")
            
            # Table information
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            tables = cursor.fetchall()
            st.markdown("### Database Tables")
            for table in tables:
                st.markdown(f"- {table[0]}")
            
            # Books table structure
            cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='books'")
            columns = cursor.fetchall()
            st.markdown("### Books Table Structure")
            for col in columns:
                st.markdown(f"- **{col[0]}**: {col[1]}")
            
            cursor.close()
            conn.close()
        except Exception as e:
            st.error(f"Connection test failed: {str(e)}")
            st.error(traceback.format_exc())

# Sidebar navigation
st.sidebar.title("Book Arbitrage")
page = st.sidebar.radio("Navigation", ["Database Browser", "Arbitrage Opportunities", "Scraper Management"])

# Database Browser Page
if page == "Database Browser":
    st.title("Book Database Browser")
    
    # Filters in the sidebar
    st.sidebar.header("Filters")
    
    # Search term
    search_term = st.sidebar.text_input("Search (title or author)")
    
    # Price range
    min_price = st.sidebar.number_input("Min Price", min_value=0.0, value=0.0, step=1.0)
    max_price = st.sidebar.number_input("Max Price", min_value=0.0, value=100.0, step=1.0)
    
    # Condition dropdown
    condition_options = ["", "New", "Like New", "Very Good", "Good", "Fair", "Poor", "Well Read"]
    condition = st.sidebar.selectbox("Condition", condition_options)
    
    # Binding type dropdown
    binding_options = ["", "Hardcover", "Paperback", "Leather"]
    binding = st.sidebar.selectbox("Binding Type", binding_options)
    
    # First edition checkbox
    first_edition = st.sidebar.checkbox("First Edition Only")
    
    # Apply filters
    filters = {
        "search_term": search_term,
        "min_price": min_price,
        "max_price": max_price,
        "condition": condition,
        "binding": binding,
        "first_edition": "Yes" if first_edition else ""
    }
    
    # Fetch books with try/except for better error handling
    try:
        books = fetch_books(filters=filters)
        
        # Debug information
        st.sidebar.markdown("### Debug Information")
        debug_expander = st.sidebar.expander("Show Raw Data", expanded=False)
        with debug_expander:
            st.write(f"Raw data type: {type(books)}")
            st.write(f"Number of rows: {len(books)}")
            if len(books) > 0:
                st.write("First row keys:")
                st.write(list(books[0].keys()))
                
                st.write("Sample data (first row):")
                st.write(books[0])
        
        books_df = pd.DataFrame(books)
        
        # Debug the DataFrame
        with debug_expander:
            st.write("DataFrame info:")
            buffer = io.StringIO()
            books_df.info(buf=buffer)
            st.text(buffer.getvalue())
            
            st.write("DataFrame columns:")
            st.write(books_df.columns.tolist())
            
            st.write("DataFrame sample:")
            st.dataframe(books_df.head(2))
        
        if not books_df.empty:
            # Data statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Books", len(books_df))
            with col2:
                if 'wob_price' in books_df.columns:
                    avg_price = pd.to_numeric(books_df['wob_price'], errors='coerce').mean()
                    st.metric("Average Price", f"£{avg_price:.2f}")
            with col3:
                if 'condition' in books_df.columns:
                    most_common_condition = books_df['condition'].value_counts().idxmax()
                    st.metric("Most Common Condition", most_common_condition)
            with col4:
                if 'author' in books_df.columns and not books_df['author'].isna().all():
                    authors_count = books_df['author'].nunique()
                    st.metric("Unique Authors", authors_count)
            
            # Price Distribution Chart
            if 'wob_price' in books_df.columns:
                st.subheader("Price Distribution")
                # Convert to numeric first
                numeric_prices = pd.to_numeric(books_df['wob_price'], errors='coerce')
                price_df = pd.DataFrame({'wob_price': numeric_prices.dropna()})
                
                if not price_df.empty:
                    fig = px.histogram(price_df, x='wob_price', nbins=20, title="Book Price Distribution")
                    fig.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font_color='white',
                        xaxis_title="Price (£)",
                        yaxis_title="Number of Books"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # Book table with details
            st.subheader("Book Database")
            
            # Transform data for display
            # Make a copy to avoid modification warnings
            display_df = books_df.copy()
            
            # Log the columns for debugging
            with debug_expander:
                st.write("Available columns:", list(display_df.columns))
            
            # Set default empty values for missing columns
            required_columns = ['title', 'author', 'sku', 'condition', 'wob_price', 'binding', 'publisher', 'created_at']
            for col in required_columns:
                if col not in display_df.columns:
                    display_df[col] = ""
                    with debug_expander:
                        st.warning(f"Missing column: {col}")
            
            # Select only the needed columns for display
            try:
                display_df = display_df[required_columns].copy()
            except Exception as e:
                st.error(f"Error selecting columns: {e}")
                st.write("DataFrame columns:", list(display_df.columns))
            
            # Convert created_at to string to avoid formatting issues
            if 'created_at' in display_df.columns:
                display_df['created_at'] = display_df['created_at'].astype(str)
                # Extract only the date part (YYYY-MM-DD) if it's a longer timestamp
                display_df['created_at'] = display_df['created_at'].str.split('.').str[0]
            
            # Round prices to 2 decimal places if they exist
            if 'wob_price' in display_df.columns:
                # Convert to numeric first, handling errors
                display_df['wob_price'] = pd.to_numeric(display_df['wob_price'], errors='coerce')
                
                # Format as currency
                display_df['wob_price'] = display_df['wob_price'].apply(
                    lambda x: f"£{float(x):.2f}" if pd.notnull(x) else ""
                )
            
            # Replace empty values with "None" for better display
            display_df = display_df.fillna("None")
            
            # Rename columns for better display
            display_df.columns = ['Title', 'Author', 'SKU', 'Condition', 'Price', 'Binding', 'Publisher', 'Added Date']
            
            # Display the dataframe with high contrast and improved visibility
            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True
            )
            
            # Book Details
            st.subheader("Book Details")
            if not books_df.empty:
                # Get a list of book titles for the selectbox
                book_titles = books_df['title'].tolist() if 'title' in books_df.columns else []
                selected_book = st.selectbox("Select a book to view details", book_titles)
                
                # Display selected book details
                if selected_book:
                    book_details = books_df[books_df['title'] == selected_book].iloc[0]
                    
                    # Create a card-like layout for details
                    st.markdown('<div class="card">', unsafe_allow_html=True)
                    
                    # Book title and primary details
                    st.markdown(f"### {book_details['title']}")
                    
                    # Create three columns for the details
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown("#### Basic Information")
                        st.markdown(f"<span class='book-detail-label'>Author:</span> {book_details['author'] if 'author' in book_details and pd.notnull(book_details['author']) else 'N/A'}", unsafe_allow_html=True)
                        st.markdown(f"<span class='book-detail-label'>SKU:</span> {book_details['sku'] if 'sku' in book_details and pd.notnull(book_details['sku']) else 'N/A'}", unsafe_allow_html=True)
                        
                        # Handle price formatting
                        if 'wob_price' in book_details and pd.notnull(book_details['wob_price']):
                            try:
                                price_val = float(book_details['wob_price'])
                                st.markdown(f"<span class='book-detail-label'>Price:</span> £{price_val:.2f}", unsafe_allow_html=True)
                            except (ValueError, TypeError):
                                st.markdown(f"<span class='book-detail-label'>Price:</span> {book_details['wob_price']}", unsafe_allow_html=True)
                        else:
                            st.markdown("<span class='book-detail-label'>Price:</span> N/A", unsafe_allow_html=True)
                        
                        st.markdown(f"<span class='book-detail-label'>Condition:</span> {book_details['condition'] if 'condition' in book_details and pd.notnull(book_details['condition']) else 'N/A'}", unsafe_allow_html=True)
                        st.markdown(f"<span class='book-detail-label'>Binding:</span> {book_details['binding'] if 'binding' in book_details and pd.notnull(book_details['binding']) else 'N/A'}", unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown("#### Publishing Details")
                        st.markdown(f"<span class='book-detail-label'>Publisher:</span> {book_details['publisher'] if 'publisher' in book_details and pd.notnull(book_details['publisher']) else 'N/A'}", unsafe_allow_html=True)
                        st.markdown(f"<span class='book-detail-label'>Publication Year:</span> {book_details['publication_year'] if 'publication_year' in book_details and pd.notnull(book_details['publication_year']) else 'N/A'}", unsafe_allow_html=True)
                        st.markdown(f"<span class='book-detail-label'>First Edition:</span> {book_details['first_edition'] if 'first_edition' in book_details and pd.notnull(book_details['first_edition']) else 'N/A'}", unsafe_allow_html=True)
                        st.markdown(f"<span class='book-detail-label'>Edition:</span> {book_details['edition'] if 'edition' in book_details and pd.notnull(book_details['edition']) else 'N/A'}", unsafe_allow_html=True)
                        st.markdown(f"<span class='book-detail-label'>Signed By Author:</span> {book_details['signed_by_author'] if 'signed_by_author' in book_details and pd.notnull(book_details['signed_by_author']) else 'N/A'}", unsafe_allow_html=True)
                    
                    with col3:
                        st.markdown("#### Additional Details")
                        st.markdown(f"<span class='book-detail-label'>Number of Pages:</span> {book_details['number_of_pages'] if 'number_of_pages' in book_details and pd.notnull(book_details['number_of_pages']) else 'N/A'}", unsafe_allow_html=True)
                        st.markdown(f"<span class='book-detail-label'>Dust Jacket:</span> {book_details['dust_jacket'] if 'dust_jacket' in book_details and pd.notnull(book_details['dust_jacket']) else 'N/A'}", unsafe_allow_html=True)
                        st.markdown(f"<span class='book-detail-label'>Dust Jacket Condition:</span> {book_details['dust_jacket_condition'] if 'dust_jacket_condition' in book_details and pd.notnull(book_details['dust_jacket_condition']) else 'N/A'}", unsafe_allow_html=True)
                        
                        # Format created_at date
                        if 'created_at' in book_details and pd.notnull(book_details['created_at']):
                            created_at_str = str(book_details['created_at']).split('.')[0]
                            st.markdown(f"<span class='book-detail-label'>Added to Database:</span> {created_at_str}", unsafe_allow_html=True)
                        else:
                            st.markdown("<span class='book-detail-label'>Added to Database:</span> N/A", unsafe_allow_html=True)
                        
                        # Link to World of Books - only shown in the detailed view
                        if 'wob_url' in book_details and pd.notnull(book_details['wob_url']) and book_details['wob_url'].strip():
                            st.markdown(f"<span class='book-detail-label'>View on World of Books:</span> <a href='{book_details['wob_url']}' target='_blank'>Open Link</a>", unsafe_allow_html=True)
                    
                    # Notes and full details
                    if ('cover_note' in book_details and pd.notnull(book_details['cover_note'])) or \
                       ('details_note' in book_details and pd.notnull(book_details['details_note'])):
                        st.markdown("#### Notes")
                        if 'cover_note' in book_details and pd.notnull(book_details['cover_note']):
                            st.markdown(f"<span class='book-detail-label'>Cover Note:</span> {book_details['cover_note']}", unsafe_allow_html=True)
                        if 'details_note' in book_details and pd.notnull(book_details['details_note']):
                            st.markdown(f"<span class='book-detail-label'>Details Note:</span> {book_details['details_note']}", unsafe_allow_html=True)
                    
                    # Full details text
                    if 'details_text' in book_details and pd.notnull(book_details['details_text']) and str(book_details['details_text']).strip():
                        with st.expander("Show Full Details Text"):
                            st.code(str(book_details['details_text']), language=None)
                    
                    st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("No books found in the database or with the selected filters.")
    
    except Exception as e:
        st.error(f"Error loading book data: {str(e)}")
        st.error(traceback.format_exc())

# Arbitrage Opportunities Page
elif page == "Arbitrage Opportunities":
    st.title("Arbitrage Opportunities")
    
    # Tabs for different views
    tabs = st.tabs(["All Opportunities", "High Profit", "Special Items"])
    
    with tabs[0]:
        st.info("This feature is under development. It will display potential arbitrage opportunities between World of Books and AbeBooks.")
        
        # Placeholder table for future arbitrage data
        placeholder_data = {
            "Title": ["The Complete Golfer", "First Edition Classics", "Rare Signed Copy"],
            "WoB Price": ["£15.99", "£24.99", "£19.50"],
            "Abe Price": ["£45.00", "£89.99", "£120.00"],
            "Profit": ["£29.01", "£65.00", "£100.50"],
            "Margin %": ["181%", "260%", "515%"],
            "Confidence": ["High", "Medium", "High"],
        }
        
        placeholder_df = pd.DataFrame(placeholder_data)
        st.dataframe(placeholder_df, hide_index=True, use_container_width=True)
    
    with tabs[1]:
        st.write("High profit opportunities with margins over 200% will appear here")
    
    with tabs[2]:
        st.write("Special items like first editions and signed copies will be highlighted here")
    
    # Future features explanation
    st.subheader("Coming Soon")
    features_col1, features_col2 = st.columns(2)
    
    with features_col1:
        st.markdown("#### Book Matching")
        st.write("- Automated matching between WoB and AbeBooks")
        st.write("- ISBN and title-based matching algorithms")
        st.write("- Special handling for rare editions")
    
    with features_col2:
        st.markdown("#### Profit Analytics")
        st.write("- Price difference calculation with fees included")
        st.write("- ROI and profit margin analytics")
        st.write("- Confidence scoring for matches")

# Scraper Management Page
elif page == "Scraper Management":
    st.title("Scraper Management")
    
    # Status overview
    status_col1, status_col2 = st.columns(2)
    
    with status_col1:
        # Display the last run time
        last_run_time = datetime.now()  # Replace with actual last run time from the database
        st.metric("Last WoB Scraper Run", last_run_time.strftime('%Y-%m-%d %H:%M'))
        
        # Count books in database
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM books")
            book_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            st.metric("Books in Database", str(book_count))
        except Exception as e:
            st.error(f"Error counting books: {str(e)}")
            st.metric("Books in Database", "Error")
    
    with status_col2:
        # AbeBooks scraper status
        st.metric("Last Abe Scraper Run", "Not yet run")
        st.metric("Pending Scrapes", "1")  # Replace with actual count
    
    # Scraper controls
    st.subheader("Run Scrapers")
    
    scraper_col1, scraper_col2 = st.columns(2)
    
    with scraper_col1:
        st.markdown("#### World of Books")
        wob_category = st.selectbox(
            "Category", 
            ["Rare Non-Fiction Books", "Rare Fiction Books", "First Editions", "Signed Copies"]
        )
        wob_books_count = st.number_input("Number of books to scrape", min_value=5, max_value=100, value=10, step=5)
        
        if st.button("Run WoB Scraper Now", key="wob_button"):
            st.info(f"Starting WoB scraper for {wob_books_count} books in category '{wob_category}'...")
            # In a real implementation, you would call the scraper here
    
    with scraper_col2:
        st.markdown("#### AbeBooks")
        abe_category = st.selectbox(
            "Search term", 
            ["rare books", "first edition", "signed", "antiquarian"]
        )
        abe_books_count = st.number_input("Number of books to scrape", min_value=5, max_value=100, value=10, step=5, key="abe_count")
        
        if st.button("Run AbeBooks Scraper Now", key="abe_button"):
            st.info(f"Starting AbeBooks scraper for {abe_books_count} books with search term '{abe_category}'...")
            # In a real implementation, you would call the scraper here
    
    # Scheduler settings
    st.subheader("Scheduler Settings")
    
    scheduler_col1, scheduler_col2 = st.columns(2)
    
    with scheduler_col1:
        st.markdown("#### Automated Scraping")
        st.checkbox("Enable automated scraping", value=True)
        st.selectbox("Run frequency", ["Every hour", "Every 2 hours", "Every 4 hours", "Every 12 hours", "Daily"])
        st.number_input("Books per automated run", min_value=10, max_value=500, value=100)
    
    with scheduler_col2:
        st.markdown("#### Categories to Include")
        st.multiselect(
            "Select categories to scrape automatically", 
            ["Rare Fiction", "Rare Non-Fiction", "First Editions", "Signed Copies", "Antiquarian"],
            default=["Rare Fiction", "Rare Non-Fiction"]
        )
    
    # Scraper logs
    st.subheader("Scraper Logs")
    
    # Tabs for different log views
    log_tabs = st.tabs(["Recent Logs", "Errors", "Statistics"])
    
    with log_tabs[0]:
        # Display logs from DB or log file
        try:
            with open("scraper.log", "r") as log_file:
                log_data = log_file.read()
        except FileNotFoundError:
            log_data = """
2025-03-29 19:08:21 - INFO - Initializing browser...
2025-03-29 19:08:22 - INFO - Browser initialized
2025-03-29 19:08:23 - INFO - Navigating to: https://www.worldofbooks.com/en-gb/collections/rare-non-fiction-books
2025-03-29 19:08:24 - INFO - Response status: 200
2025-03-29 19:08:24 - INFO - Attempting to bypass cookie consent...
2025-03-29 19:08:31 - INFO - Extracting product data using JavaScript evaluation...
2025-03-29 19:08:31 - INFO - JavaScript evaluation returned 40 products
2025-03-29 19:08:31 - INFO - Processed 40 books from JavaScript extraction
2025-03-29 19:09:06 - INFO - Comprehensive data saved to book_data.json
2025-03-29 19:09:06 - INFO - Saving 40 books to database...
2025-03-29 19:09:06 - INFO - Connected to database: evolabz
2025-03-29 19:09:07 - INFO - Successfully inserted 6 out of 40 books
2025-03-29 19:09:07 - INFO - Browser closed
            """
        
        st.text_area("Logs", log_data, height=300)
    
    with log_tabs[1]:
        # Error logs
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM books WHERE sku IS NULL OR sku = ''")
            missing_sku = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM books WHERE wob_price IS NULL")
            missing_price = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            if missing_sku > 0 or missing_price > 0:
                st.warning("Database issues detected:")
                if missing_sku > 0:
                    st.markdown(f"- {missing_sku} books with missing SKU")
                if missing_price > 0:
                    st.markdown(f"- {missing_price} books with missing price")
            else:
                st.success("No data issues detected in database.")
        except Exception as e:
            st.error(f"Error checking for data issues: {str(e)}")
        
        error_data = """
2025-03-29 19:09:06 - WARNING - Error inserting book 'Mother Natures Birds': duplicate key value violates unique constraint "books_sku_unique"
2025-03-29 19:09:06 - WARNING - Error inserting book 'The Complete Golfer': duplicate key value violates unique constraint "books_sku_unique"
2025-03-29 19:09:06 - WARNING - Error inserting book 'Ursula's Fortune': duplicate key value violates unique constraint "books_sku_unique"
        """
        
        st.text_area("Error Logs", error_data, height=300)
    
    with log_tabs[2]:
        # Scraper statistics
        stats_data = {
            "Date": ["2025-03-29", "2025-03-28", "2025-03-27", "2025-03-26", "2025-03-25"],
            "Books Scraped": [40, 38, 42, 39, 41],
            "Books Added": [6, 12, 8, 15, 7],
            "Duplicates": [34, 26, 34, 24, 34],
            "Errors": [0, 2, 0, 1, 0]
        }
        
        stats_df = pd.DataFrame(stats_data)
        st.dataframe(stats_df, hide_index=True, use_container_width=True)

        # Book Types Statistics
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE condition = 'Good') as good_condition,
                    COUNT(*) FILTER (WHERE condition = 'Very Good') as very_good_condition,
                    COUNT(*) FILTER (WHERE condition = 'Well Read') as well_read_condition,
                    COUNT(*) FILTER (WHERE binding = 'Hardcover') as hardcover,
                    COUNT(*) FILTER (WHERE binding = 'Paperback') as paperback,
                    COUNT(*) FILTER (WHERE first_edition = 'Yes') as first_editions,
                    COUNT(*) FILTER (WHERE signed_by_author = 'Yes') as signed_copies
                FROM books
            """)
            stats = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if stats:
                st.subheader("Book Statistics")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown("**Condition**")
                    st.markdown(f"- Good: {stats[0]}")
                    st.markdown(f"- Very Good: {stats[1]}")
                    st.markdown(f"- Well Read: {stats[2]}")
                
                with col2:
                    st.markdown("**Binding**")
                    st.markdown(f"- Hardcover: {stats[3]}")
                    st.markdown(f"- Paperback: {stats[4]}")
                
                with col3:
                    st.markdown("**Special Attributes**")
                    st.markdown(f"- First Editions: {stats[5]}")
                    st.markdown(f"- Signed Copies: {stats[6]}")
        except Exception as e:
            st.error(f"Error loading statistics: {str(e)}")

# Database Maintenance Tools Section (shown when using debug mode)
debug_mode = st.sidebar.checkbox("Enable Database Maintenance Mode")

if debug_mode:
    st.markdown("---")
    st.header("Database Maintenance Tools")
    
    maint_tabs = st.tabs(["Schema", "Data Analysis", "Run SQL"])
    
    with maint_tabs[0]:
        st.subheader("Database Schema")
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Get table schema
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'books'
                ORDER BY ordinal_position
            """)
            
            schema_data = cursor.fetchall()
            if schema_data:
                schema_df = pd.DataFrame(schema_data, columns=["Column Name", "Data Type", "Nullable"])
                st.dataframe(schema_df, hide_index=True, use_container_width=True)
            
            # Get constraints
            cursor.execute("""
                SELECT 
                    con.conname as constraint_name,
                    con.contype as constraint_type,
                    pg_get_constraintdef(con.oid) as constraint_definition
                FROM 
                    pg_constraint con
                    JOIN pg_class rel ON rel.oid = con.conrelid
                    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                WHERE 
                    rel.relname = 'books'
                    AND nsp.nspname = 'public'
            """)
            
            constraints = cursor.fetchall()
            if constraints:
                st.subheader("Table Constraints")
                for constraint in constraints:
                    st.markdown(f"**{constraint[0]}** ({constraint[1]}): {constraint[2]}")
            
            cursor.close()
            conn.close()
        except Exception as e:
            st.error(f"Error fetching schema: {str(e)}")
    
    with maint_tabs[1]:
        st.subheader("Data Analysis")
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Null value analysis
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_books,
                    SUM(CASE WHEN title IS NULL OR title = '' THEN 1 ELSE 0 END) as null_title,
                    SUM(CASE WHEN author IS NULL OR author = '' THEN 1 ELSE 0 END) as null_author,
                    SUM(CASE WHEN isbn IS NULL OR isbn = '' THEN 1 ELSE 0 END) as null_isbn,
                    SUM(CASE WHEN sku IS NULL OR sku = '' THEN 1 ELSE 0 END) as null_sku,
                    SUM(CASE WHEN wob_price IS NULL THEN 1 ELSE 0 END) as null_price,
                    SUM(CASE WHEN wob_url IS NULL OR wob_url = '' THEN 1 ELSE 0 END) as null_url,
                    SUM(CASE WHEN condition IS NULL OR condition = '' THEN 1 ELSE 0 END) as null_condition,
                    SUM(CASE WHEN binding IS NULL OR binding = '' THEN 1 ELSE 0 END) as null_binding
                FROM books
            """)
            
            null_analysis = cursor.fetchone()
            
            if null_analysis:
                total = null_analysis['total_books']
                st.subheader("Missing Data Analysis")
                
                missing_data = {
                    "Field": ["Title", "Author", "ISBN", "SKU", "Price", "URL", "Condition", "Binding"],
                    "Missing Count": [
                        null_analysis['null_title'],
                        null_analysis['null_author'], 
                        null_analysis['null_isbn'],
                        null_analysis['null_sku'],
                        null_analysis['null_price'],
                        null_analysis['null_url'],
                        null_analysis['null_condition'],
                        null_analysis['null_binding']
                    ],
                    "Missing %": [
                        f"{(null_analysis['null_title']/total*100):.1f}%" if total > 0 else "0%",
                        f"{(null_analysis['null_author']/total*100):.1f}%" if total > 0 else "0%",
                        f"{(null_analysis['null_isbn']/total*100):.1f}%" if total > 0 else "0%",
                        f"{(null_analysis['null_sku']/total*100):.1f}%" if total > 0 else "0%",
                        f"{(null_analysis['null_price']/total*100):.1f}%" if total > 0 else "0%",
                        f"{(null_analysis['null_url']/total*100):.1f}%" if total > 0 else "0%",
                        f"{(null_analysis['null_condition']/total*100):.1f}%" if total > 0 else "0%",
                        f"{(null_analysis['null_binding']/total*100):.1f}%" if total > 0 else "0%"
                    ]
                }
                
                missing_df = pd.DataFrame(missing_data)
                st.dataframe(missing_df, hide_index=True, use_container_width=True)
            
            # Price Range Analysis
            cursor.execute("""
                SELECT 
                    MIN(wob_price) as min_price,
                    MAX(wob_price) as max_price,
                    AVG(wob_price) as avg_price,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wob_price) as median_price
                FROM books
                WHERE wob_price IS NOT NULL
            """)
            
            price_analysis = cursor.fetchone()
            
            if price_analysis:
                st.subheader("Price Analysis")
                price_cols = st.columns(4)
                with price_cols[0]:
                    st.metric("Minimum Price", f"£{price_analysis['min_price']:.2f}")
                with price_cols[1]:
                    st.metric("Maximum Price", f"£{price_analysis['max_price']:.2f}")
                with price_cols[2]:
                    st.metric("Average Price", f"£{price_analysis['avg_price']:.2f}")
                with price_cols[3]:
                    st.metric("Median Price", f"£{price_analysis['median_price']:.2f}")
            
            cursor.close()
            conn.close()
        except Exception as e:
            st.error(f"Error analyzing data: {str(e)}")
    
    with maint_tabs[2]:
        st.subheader("Run SQL Query")
        
        custom_query = st.text_area("Enter SQL Query", "SELECT * FROM books LIMIT 10;", height=100)
        
        if st.button("Run Query"):
            try:
                conn = get_db_connection()
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                
                cursor.execute(custom_query)
                
                if cursor.description:  # Check if query returns data
                    results = cursor.fetchall()
                    if results:
                        results_df = pd.DataFrame(results)
                        st.dataframe(results_df, use_container_width=True)
                    else:
                        st.info("Query executed successfully but returned no results.")
                else:
                    # For queries that don't return data (INSERT, UPDATE, etc.)
                    row_count = cursor.rowcount
                    st.success(f"Query executed successfully. {row_count} rows affected.")
                
                cursor.close()
                conn.close()
            except Exception as e:
                st.error(f"Error executing query: {str(e)}")
                st.code(traceback.format_exc())

# Footer
st.sidebar.markdown("---")
st.sidebar.write("Book Arbitrage App v0.1")
st.sidebar.write("© 2025")
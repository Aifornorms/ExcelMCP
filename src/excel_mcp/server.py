import logging
import sys
import os
from os import path
from typing import Any, List, Dict, Optional
from mcp.server.fastmcp import FastMCP, Context
import pandas as pd
import matplotlib.pyplot as plt
from .data_handlers import ExcelDataHandler as ExcelHandler

os.environ["MODIN_ENGINE"] = "dask"
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("excel-mcp.log")],
    force=True,
)

logger = logging.getLogger("excel-mcp")

# Get Excel files path from environment or use default
EXCEL_FILES_PATH = os.environ.get("EXCEL_FILES_PATH", "./excel_files")

# Create the directory if it doesn't exist
os.makedirs(EXCEL_FILES_PATH, exist_ok=True)

# Initialize FastMCP server
mcp = FastMCP(
    "excel-mcp",
    version="0.1.0",
    description="MCP server for Excel and CSV file operations, file paths should use relative addresses",
)

@mcp.tool()
def list_worksheets(filepath: str) -> List[str]:
    """Get all worksheet names from the specified Excel or CSV file.

    Args:
        filepath: Relative or absolute path to the target file

    Returns:
        List[str]: List containing all worksheet names

    Raises:
        FileNotFoundError: If the specified file path does not exist
        ValueError: If the file format is invalid or not an Excel/CSV file
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        sheet_names = excel_handler.get_sheet_names(filepath)
        return f"Total {len(sheet_names)}\n" + "\n".join(sheet_names)
    except Exception as e:
        logger.error(f"Error getting sheet names: {e}")
        raise


@mcp.tool()
def list_columns(filepath: str, sheet_name: str) -> str:
    """Get all column names and their data types from the specified worksheet in the file.

    Args:
        filepath: Relative or absolute path to the target file
        sheet_name: Name of the worksheet to get column names from (for CSV files, this parameter will be ignored)

    Returns:
        str: Formatted string containing column names and data types

    Raises:
        FileNotFoundError: If the specified file path does not exist
        ValueError: If the specified worksheet name does not exist
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        columns = df.columns.tolist()
        dtypes = df.dtypes
        # Calculate maximum column name length for alignment
        max_col_len = max(len(str(col)) for col in columns) if columns else 0
        max_col_len = max(max_col_len, 10)  # Minimum width is 10

        # Generate formatted table output
        header = f"Total {len(columns)} columns\n" + "-" * (max_col_len + 10) + "\n"
        header += f"{'Column Name'.ljust(max_col_len)}    Type\n"
        header += "-" * (max_col_len + 10) + "\n"

        rows = [f"{str(col).ljust(max_col_len)}    {dtypes[col]}" for col in columns]
        return header + "\n".join(rows)
    except Exception as e:
        logger.error(f"Error getting Excel columns: {e}")
        raise


@mcp.tool()
def get_random_sample(filepath: str, sheet_name: str, sample_size: int) -> str:
    """Get random sample data from Excel or CSV file.

    Args:
        filepath: Relative or absolute path to the target file
        sheet_name: Name of the worksheet to sample from (for CSV files, this parameter will be ignored)
        sample_size: Number of rows to sample

    Returns:
        str: String representation of the random sample data

    Raises:
        FileNotFoundError: If the specified file path does not exist
        ValueError: Thrown when sample size is greater than dataset size
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        sample_df = excel_handler.get_random_sample(
            df, sample_size, sheet_name=sheet_name
        )

        return sample_df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        logger.error(f"Error getting random sample: {e}")
        raise


@mcp.tool()
def analyze_missing_values(filepath: str, sheet_name: str) -> str:
    """Get missing data information from Excel or CSV file.
    Args:
        filepath: Relative or absolute path to the target file
        sheet_name: Name of the worksheet to analyze (for CSV files, this parameter will be ignored)
    Returns:
        str: Detailed statistical information containing the number and rate of missing values for each column
    Raises:
        FileNotFoundError: If the specified file path does not exist
        ValueError: If the worksheet does not exist or the file format is invalid
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        return excel_handler.get_missing_values_info(df)
    except Exception as e:
        logger.error(f"Error getting Excel sheet missing values info: {e}")
        raise


@mcp.tool()
def analyze_unique_values(
    filepath: str,
    sheet_name: str,
    max_unique: int = 10,
) -> Dict[str, Any]:
    """Get the distribution of unique values for specified columns in an Excel or CSV file.

    Args:
        filepath: Relative or absolute path to the target file
        sheet_name: Name of the worksheet to analyze (for CSV files, this parameter will be ignored)
        max_unique: Maximum number of unique values to display for each column, beyond this number only statistics are shown

    Returns:
        Dict[str, Any]: Dictionary containing detailed information about the distribution of unique values for each column

    Raises:
        FileNotFoundError: If the specified file path does not exist
        ValueError: If the worksheet does not exist or the specified column name does not exist
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        return excel_handler.get_data_unique_values(
            df, columns=None, max_unique=max_unique
        )
    except Exception as e:
        logger.error(f"Error getting Excel sheet unique values: {e}")
        raise


@mcp.tool()
def analyze_data_overview(filepath: str, sheet_name: str) -> str:
    """Data analysis first choice: Get a complete data overview of the worksheet. Perform comprehensive data analysis, 
    including data type statistics, missing value analysis, non-null value counts and other key metric checks.
    
    Args:
        filepath: Relative or absolute path to the target file
        sheet_name: Name of the worksheet to analyze (for CSV files, this parameter will be ignored)
    
    Returns:
        str: Detailed information string containing worksheet data analysis results
    
    Raises:
        FileNotFoundError: If the specified file path does not exist
        ValueError: If the worksheet does not exist or the file format is invalid
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        # Read data
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        # Get row and column data
        num_rows, num_cols = df.shape
        # Get missing values information
        missing_values_info = excel_handler.get_missing_values_info(df)

        # Convert data type information to string format
        dtypes_str = "\n".join(
            [f"    {col}: {dtype}" for col, dtype in df.dtypes.items()]
        )                

        # Build the final result string
        result = f"""
Data Analysis Results:
1. Data Scale:
    Total Rows: {num_rows}
    Total Columns: {num_cols}

2. Data Types:
{dtypes_str}
"""
        return result
    except Exception as e:
        error_msg = f"Error inspecting Excel sheet data: {e}"
        logger.error(error_msg)
        return error_msg


@mcp.tool()
def analyze_correlations(
    filepath: str,
    sheet_name: str,
    method: str = "pearson",
    min_correlation: float = 0.5,
) -> str:
    """Get correlations between columns in an Excel or CSV file.

    Args:
        filepath: Relative or absolute path to the target file
        sheet_name: Name of the worksheet to analyze (for CSV files, this parameter will be ignored)
        method: Correlation analysis method, supports 'pearson', 'spearman', 'kendall'
        min_correlation: Correlation coefficient threshold, only returns results where the absolute value of the correlation coefficient is greater than this value

    Returns:
        str: Detailed result string containing column correlation analysis

    Raises:
        FileNotFoundError: If the specified file path does not exist
        ValueError: If the worksheet does not exist, invalid correlation calculation method, or data types are not suitable for correlation calculation
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        sheet_names = excel_handler.get_sheet_names(filepath)
        if sheet_name not in sheet_names:
            raise ValueError(f"Sheet '{sheet_name}' not found. Available sheets: {sheet_names}")
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        return excel_handler.get_column_correlation(df, method, min_correlation)
    except FileNotFoundError as e:
        logger.error(f"File not found: {filepath}")
        return f"Error: File not found: {filepath}"
    except ValueError as e:
        logger.error(f"Invalid sheet name or file format: {e}")
        return f"Error: {str(e)}"
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return f"Error: {str(e)}"


@mcp.tool()
def analyze_numeric_stats(
        filepath: str, sheet_name: str, columns: List[str]
    ) -> Dict[str, Any]:
        """Get statistical information for numeric columns, including mean, median, standard deviation, quantiles, etc.

        Args:
            filepath: Source file path
            sheet_name: Worksheet name (for CSV files, this parameter will be ignored)
            columns: List of column names to analyze, defaults to all numeric columns, at least one column. All columns must be numeric type. Maximum of 10 columns.

        Returns:
            Dict[str, Any]: Dictionary containing statistical information for each numeric column
        """
        excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
        try:
            df = excel_handler.read_data(
                excel_handler.get_file_path(filepath), sheet_name=sheet_name
            )
            numerical_cols = (
                df.select_dtypes(include=["int64", "float64"]).columns
                if columns is None
                else columns
            )

            # Get basic statistical information
            stats = df[numerical_cols].describe()

            # Calculate the sum of numeric columns
            sums = df[numerical_cols].sum(numeric_only=True, skipna=True)
            stats.loc["sum"] = sums

            return stats.to_json(orient="records", force_ascii=False)
        except Exception as e:
            logger.error(f"Error calculating statistics: {e}")
            raise


@mcp.tool()
def analyze_group_stats(
    filepath: str,
    sheet_name: str,
    group_by: str,
    agg_columns: List[str],
    agg_functions: List[str] = ["mean", "count"],
) -> str:
    """Group by specified column and calculate statistics.

    Args:
        filepath: Source file path
        sheet_name: Worksheet name (for CSV files, this parameter will be ignored)
        group_by: Column name to group by
        agg_columns: List of column names to calculate statistics for
        agg_functions: List of statistical functions, supports 'mean', 'sum', 'count', 'min', 'max', etc.

    Returns:
        str: String representation of group statistics results
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        grouped = df.groupby(group_by)[agg_columns].agg(agg_functions)
        # Sort by the result of the first aggregation function of the first statistical column in descending order
        first_col = agg_columns[0]
        first_func = agg_functions[0]
        sort_col = (
            (first_col, first_func)
            if isinstance(grouped.columns, pd.MultiIndex)
            else first_col
        )
        sorted_grouped = grouped.sort_values(by=sort_col, ascending=False)
        return sorted_grouped.to_string()
    except Exception as e:
        logger.error(f"Error in group statistics: {e}")
        raise


@mcp.tool()
def analyze_time_series(
    filepath: str, sheet_name: str, date_column: str, value_column: str, freq: str = "M"
) -> str:
    """Analyze time series data, including trends, seasonality, etc.

    Args:
        filepath: Source file path
        sheet_name: Worksheet name (for CSV files, this parameter will be ignored)
        date_column: Date column name
        value_column: Value column name
        freq: Resampling frequency, such as 'D' (day), 'M' (month), 'Y' (year)

    Returns:
        str: Time series analysis results
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        df[date_column] = pd.to_datetime(df[date_column])
        df = df.set_index(date_column)

        # Resample and calculate statistics
        resampled = df[value_column].resample(freq).agg(["mean", "min", "max", "count"])

        return f"Time series analysis results (frequency: {freq}):\n{resampled.to_string()}"
    except Exception as e:
        logger.error(f"Error in time series analysis: {e}")
        raise


@mcp.tool()
def analyze_data(filepath: str, sheet_name: str, python_code: str) -> str:
    """
    Data analysis tool for executing Python code and capturing output.
    This tool is designed for data exploration and debugging, allowing analysis of DataFrame data through custom Python code execution. Output from all print statements will be captured and returned, making it easy to view intermediate calculation results.

    Usage limitations:
        Only for data analysis; cannot modify source files or generate new files.
        Chart drawing functionality is not supported (please use visualize_data).
        Code execution results are for display only and cannot be called by other functions.
        Recommended to optimize print output and avoid lengthy data printing.
        
    Critical Requirement: The Python code must include import pandas as pd inside the main function to use pandas functionalities. Placing import pandas as pd outside the main function is incorrect and will raise a ValueError. This ensures portability and compatibility with the execution environment.

    Filepath Handling: The filepath is resolved relative to the environment variable EXCEL_FILES_PATH (default: ./excel_files). For example, if filepath is data.csv, the tool looks for the file at ./excel_files/data.csv. Absolute paths are also supported but must be valid.

    Args:
        filepath: Path to the Excel or CSV file, either relative to EXCEL_FILES_PATH (e.g., data.csv) or absolute (e.g., /path/to/data.csv). The file must exist.
        sheet_name: Name of the worksheet to process (for CSV files, this parameter will be ignored).
        python_code: Python code containing a main function that takes a DataFrame parameter (df).
            Mandatory: The main function must include import pandas as pd as its first line to use pandas functionalities.
            Prohibited: Do not place import pandas as pd or any other imports outside the main function.
            The code must be a pure function, using only the input DataFrame (df) and avoiding side effects like reading or writing files. The df is provided by the tool, loaded from the specified filepath.

    Returns:
        str: String containing all print outputs and the return value of the main function.

    Raises:
        Taxes:
        ValueError: If the Python code has a format error, lacks a main function, places import pandas as pd outside the main function, omits import pandas as pd inside the main function, includes side effects, or if the filepath cannot be resolved (e.g., file not found).
        TypeError: If the main function’s return value type is invalid.

    Examples:
        Correct Example:
            Filepath: data.csv (resolves to ./excel_files/data.csv)
            Python Code:
                def main(df):
                    import pandas as pd
                    # Display first 5 rows of data
                    print(df.head())
                    return "Data analysis completed"
        Incorrect Example (Will Raise ValueError):
            Filepath: data.csv
            Python Code:
                import pandas as pd  # Wrong: Import outside main()
                def main(df):
                    print(df.head())
                    return "Data analysis completed"
        Incorrect Filepath Example (Will Raise ValueError):
            Filepath: nonexistent.csv (file not found at ./excel_files/nonexistent.csv)
    """

    # Initialize Excel handler
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.run_code_only_log(
            filepath, python_code, sheet_name=sheet_name
        )
    except Exception as e:
        logger.error(f"Error processing Excel file: {e}")
        raise


@mcp.tool()
def save_transformed_data(
    filepath: str,
    sheet_name: str,
    python_code: str,
    result_file_path: str,
    default_sheet_name: str = "Sheet1",
) -> str:
    """Execute Python code to generate Excel or CSV file data, supporting single or multiple sheet processing.
        This tool executes Python code to transform DataFrame data and save the results to an Excel or CSV file. The code must be a pure function, avoiding side effects such as reading external files, modifying external files, or generating visualizations. The input DataFrame ('df') represents the data from the specified filepath and sheet_name.
        
        The execution environment provides the following pre-defined variable:
            'pd': The pandas library

        Critical Requirement: 
            To ensure portability and compatibility, the python_code must include import pandas as pd inside the main function. Placing import pandas as pd outside the main function is incorrect and will raise a ValueError. This applies even though the pd variable is available in the environment.

        Args:
            filepath: Source file path
            sheet_name: Source worksheet name (for CSV files, this parameter will be ignored)
            python_code: Python code containing a main function that takes a DataFrame parameter and returns a pandas.DataFrame or Dict[str, DataFrame].
                Mandatory: The main function must include import pandas as pd as its first line to use pandas functionalities.
                Prohibited: Do not place import pandas as pd or any other imports outside the main function.
                The main function must only use the input DataFrame ('df') and avoid reading external files or generating side effects.
                When returning a DataFrame, data will be saved to the worksheet specified by default_sheet_name.
                When returning a Dict[str, DataFrame], dictionary keys are worksheet names and values are corresponding DataFrames.
            result_file_path: Path to save the result file
            default_sheet_name: Default worksheet name, used when python_code returns a single DataFrame

        Returns:
            str: Execution result information, including information about generated worksheets

        Raises:
            ValueError: If the Python code has a format error, lacks a main function, places import pandas as pd outside the main function, returns an invalid type, or includes side effects (e.g., reading external files).
            TypeError: If the main function's return value is neither a DataFrame nor a Dict[str, DataFrame].
            Examples:

        Correct Example (Single DataFrame):
            def main(df):
                import pandas as pd
                return df.groupby('category').sum()

        Incorrect Example (Will Raise ValueError):
            import pandas as pd  # Wrong: Import outside main()
            def main(df):
                return df.groupby('category').sum()

        Correct Example (Multiple Sheets):
            def main(df):
                import pandas as pd
                result = {}
                for category in df['category'].unique():
                    result[f"Sheet_{category}"] = df[df['category'] == category][['value']].copy()
                return result
    """
    # Initialize handlers

    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.run_code(
            filepath,
            python_code,
            sheet_name=sheet_name,
            result_file_path=result_file_path,
            result_sheet_name=default_sheet_name,
        )
    except Exception as e:
        logger.error(f"Error executing Excel code: {e}")
        raise


@mcp.tool()
def plot_matplotlib_chart(
    filepath: str,
    sheet_name: str,
    save_path: str,
    python_code: str,
) -> str:
    """Dedicated function for creating visualization charts from Excel or CSV data.

    Args:
        filepath: Source file path
        sheet_name: Worksheet name (for CSV files, this parameter will be ignored)
        save_path: Path to save the chart
        python_code: Python code to execute, defined as def main(df, plt), can use matplotlib for visualization, returns plt object, no need to save

    Returns:
        str: Execution result information, please provide the relative path of the result file to the user

    Raises:
        ValueError: When chart type is not supported or data column does not exist
        FileNotFoundError: When file does not exist
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.run_code_with_plot(
            filepath, python_code, save_path, sheet_name=sheet_name
        )

    except Exception as e:
        logger.error(f"Error generating chart: {e}")
        raise


@mcp.tool()
def plot_pyecharts_chart(
    filepath: str,
    sheet_name: str,
    save_path: str,
    python_code: str,
    theme: str = "light",
    title: str = None,
) -> str:
    """Dedicated function for generating a single interactive chart using Pyecharts.

    This tool executes Python code to create a single interactive chart using Pyecharts. The code must define a main(df) function that returns a single Pyecharts chart object (e.g., Bar, Line, Pie). The chart is saved as an HTML file.

    Critical Requirement: The Python code must include `import pandas as pd` and Pyecharts imports (e.g., `from pyecharts.charts import Bar`) inside the main function to ensure portability. Imports outside the main function will raise a ValueError.

    Args:
        filepath: Relative or absolute path to the Excel or CSV file
        sheet_name: Worksheet name (for CSV files, this parameter is ignored)
        save_path: Path to save the chart, must end with .html
        python_code: Python code to execute, defined as def main(df), returns a single Pyecharts chart object
        theme: Pyecharts theme (e.g., 'light', 'dark', 'chalk', 'vintage'), defaults to 'light'
        title: Optional title for the chart, displayed in the HTML page

    Returns:
        str: Execution result information, including the generated HTML file path

    Raises:
        ValueError: If save_path does not end with .html, code lacks main function, or includes invalid imports
        FileNotFoundError: If the input file does not exist
        TypeError: If the return value is not a single Pyecharts chart object

    Example:
        ```python
        def main(df):
            import pandas as pd
            from pyecharts.charts import Bar
            from pyecharts import options as opts
            bar = (
                Bar()
                .add_xaxis(df['category'].tolist())
                .add_yaxis("Values", df['value'].tolist())
                .set_global_opts(title_opts=opts.TitleOpts(title="Bar Chart"))
            )
            return bar"""
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.run_code_with_pyecharts(
        filepath, python_code, save_path, theme=theme, title=title, sheet_name=sheet_name
        )
    except Exception as e:
        logger.error(f"Error generating Pyecharts chart: {e}")
        raise


@mcp.tool()
def plot_pyecharts_dashboard(
    filepath: str,
    sheet_name: str,
    save_path: str,
    python_code: str,
    theme: str = "light",
    title: str = None,  # NEW: Added title parameter
) -> str:
    """Dedicated function for generating interactive Pyecharts dashboards with multiple charts.

    This tool executes Python code to create a dashboard with multiple charts arranged using Pyecharts Page, Grid, or Tab layouts. The code must define a main(df) function that returns a Pyecharts Page, Grid, or Tab object. The dashboard is saved as an HTML file.

    Critical Requirement: The Python code must include `import pandas as pd` and Pyecharts imports (e.g., `from pyecharts.charts import Bar`) inside the main function to ensure portability. Imports outside the main function will raise a ValueError.

    Args:
        filepath: Relative or absolute path to the Excel or CSV file
        sheet_name: Worksheet name (for CSV files, this parameter is ignored)
        save_path: Path to save the dashboard, must end with .html
        python_code: Python code to execute, defined as def main(df), returns a Pyecharts Page, Grid, or Tab object
        theme: Pyecharts theme (e.g., 'light', 'dark', 'chalk', 'vintage'), defaults to 'light'

    Returns:
        str: Execution result information, including the generated HTML file path

    Raises:
        ValueError: If save_path does not end with .html, code lacks main function, or includes invalid imports
        FileNotFoundError: If the input file does not exist
        TypeError: If the return value is not a Pyecharts Page, Grid, or Tab object

    Example:
        ```python
        def main(df):
            import pandas as pd
            from pyecharts.charts import Bar, Line
            from pyecharts import options as opts
            from pyecharts.charts import Page
            page = Page(layout=Page.SimplePageLayout)
            bar = (
                Bar()
                .add_xaxis(df['category'].tolist())
                .add_yaxis("Values", df['value'].tolist())
                .set_global_opts(title_opts=opts.TitleOpts(title="Bar Chart"))
            )
            line = (
                Line()
                .add_xaxis(df['category'].tolist())
                .add_yaxis("Values", df['value'].tolist())
                .set_global_opts(title_opts=opts.TitleOpts(title="Line Chart"))
            )
            page.add(bar, line)
            return page
            """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.run_code_with_pyecharts_dashboard(
        filepath, python_code, save_path, theme=theme, title=title, sheet_name=sheet_name
        )
    except Exception as e:
        logger.error(f"Error generating Pyecharts dashboard: {e}")
        raise


async def run_server():
    """Start Excel and CSV file processing MCP server."""
    try:
        if not os.path.exists(EXCEL_FILES_PATH):
            logger.error(f"EXCEL_FILES_PATH directory does not exist: {EXCEL_FILES_PATH}")
            raise ValueError(f"EXCEL_FILES_PATH directory does not exist: {EXCEL_FILES_PATH}")
        logger.info(f"Starting Excel/CSV MCP server (files directory: {EXCEL_FILES_PATH})")
        await mcp.run_sse_async()
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        await mcp.shutdown()
    except Exception as e:
        logger.error(f"Server failed: {e}")
        raise
    finally:
        logger.info("Server shutdown complete")

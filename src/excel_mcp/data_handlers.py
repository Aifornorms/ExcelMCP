from typing import List, Dict, Union, Optional, Callable, Any, Tuple
import numpy as np
import os
import logging
import pandas as pd
import functools
import glob
from typing import List, Dict, Any
from abc import ABC, abstractmethod
from .code_runner import run_python_code

logger = logging.getLogger("excel-mcp")

cache = {}


def cache_method(func):
    """
    Decorator that adds caching for instance methods based on file path and parameters
    Supports cache invalidation based on file's last modification time
    """
    @functools.wraps(func)
    def wrapper(self, filepath, *args, **kwargs):
        # Normalize filepath to use forward slashes for consistency
        normalized_filepath = os.path.normpath(filepath).replace(os.sep, '/')
        # Get the file's last modification time
        try:
            current_mod_time = os.path.getmtime(filepath)
        except OSError:
            # If the file doesn't exist or modification time can't be obtained, don't use cache
            return func(self, filepath, *args, **kwargs)

        # Clear outdated cache entries for this filepath
        for cached_key in list(cache.keys()):
            if cached_key[0] == normalized_filepath and cached_key[1] != current_mod_time:
                logger.debug(f"Clearing outdated cache for {cached_key}")
                del cache[cached_key]

        # Create a cache key including normalized file path, last modification time, and additional parameters
        key = (normalized_filepath, current_mod_time, frozenset(kwargs.items()))

        if key not in cache:
            # Cache miss, execute the original method and cache the result
            logger.debug(f"Cache miss {key}")
            result = func(self, filepath, *args, **kwargs)
            cache[key] = result
        return cache[key].copy()

    return wrapper


class ExcelDataHandler:
    """Excel and CSV data processing class, providing complete file operation functionality"""

    def __init__(self, files_path: str):
        self.files_path = files_path

    def get_file_path(self, filename: str) -> str:
        """Get the complete file path
        Args:
            filename: File name
        Returns:
            Complete file path
        Raises:
            FileNotFoundError: If the file does not exist
        """
        logger.debug(f"Original filename: {filename}")
        if os.path.isabs(filename):
            if not os.path.exists(filename):
                raise FileNotFoundError(f"File not found: {filename}")
            return filename
        base_filename = os.path.basename(filename)
        # Construct the default path
        full_path = os.path.join(self.files_path, base_filename)
        # Search for case-insensitive match
        pattern = os.path.join(self.files_path, base_filename).replace('\\', '/')
        matching_files = glob.glob(pattern, recursive=False)
        if matching_files:
            logger.debug(f"Found case-insensitive match: {matching_files[0]}")
            full_path = matching_files[0]
        else:
            logger.debug(f"No case-insensitive match found, using: {full_path}")
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {filename} (resolved to {full_path})")
        return full_path

    def _is_csv_file(self, filepath: str) -> bool:
        """Determine if the file is a CSV file"""
        return filepath.lower().endswith(".csv")

    @cache_method
    def read_data(
        self, filepath: str, sheet_name: str = None, **kwargs
    ) -> pd.DataFrame:
        """Read data from Excel or CSV file

        Args:
            filepath: File path
            sheet_name: Worksheet name, this parameter will be ignored for CSV files
            **kwargs: Additional parameters, will be passed to pandas reading function

        Returns:
            pd.DataFrame: Read data
        """
        if self._is_csv_file(filepath):
            return pd.read_csv(filepath, **kwargs)
        else:
            return pd.read_excel(
                filepath,
                sheet_name=sheet_name,
                engine="calamine",
                **kwargs,
            )

    def write_data(
        self, df: pd.DataFrame, filepath: str, sheet_name: str = None, **kwargs
    ) -> None:
        """Write data to Excel or CSV file

        Args:
            df: DataFrame to write
            filepath: File path
            sheet_name: Worksheet name, this parameter will be ignored for CSV files
            **kwargs: Additional parameters, will be passed to pandas writing function
        """
        if self._is_csv_file(filepath):
            df.to_csv(filepath, index=False, **kwargs)
        else:
            if os.path.exists(filepath):
                with pd.ExcelWriter(
                    filepath, mode="a", engine="openpyxl", if_sheet_exists="replace"
                ) as writer:
                    df.to_excel(
                        writer, sheet_name=sheet_name or "Sheet1", index=False, **kwargs
                    )
            else:
                with pd.ExcelWriter(filepath, mode="w", engine="openpyxl") as writer:
                    df.to_excel(
                        writer, sheet_name=sheet_name or "Sheet1", index=False, **kwargs
                    )

    def get_sheet_names(self, filepath: str) -> List[str]:
        """Get all worksheet names in an Excel file, returns ['Sheet1'] for CSV files"""
        try:
            if self._is_csv_file(filepath):
                return ["Sheet1"]
            full_path = self.get_file_path(filepath)
            excel_file = pd.ExcelFile(full_path)
            return excel_file.sheet_names
        except Exception as e:
            logger.error(f"Error getting sheet names: {e}")
            raise

    def get_columns(self, filepath: str, sheet_name: str = None) -> List[str]:
        """Get column names of the specified worksheet, for CSV files sheet_name parameter will be ignored"""
        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, sheet_name=sheet_name)
            return df.columns.tolist()
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            raise

    def run_code(
        self,
        filepath: str,
        python_code: str,
        sheet_name: str,
        result_file_path: str,
        result_sheet_name: str = None,
        **kwargs,
    ) -> str:
        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, sheet_name=sheet_name)
            # Prepare execution environment

            # Prepare execution environment with common libraries
            import pandas as pd
            import matplotlib.pyplot as plt
            from io import BytesIO
            import base64
            
            exec_locals = {
                'df': df,
                'pd': pd,
                'plt': plt,
                'BytesIO': BytesIO,
                'base64': base64,
                'filepath': full_path
            }

            # Execute Python code
            run_python_code(python_code, exec_locals)
            if "main" not in exec_locals:
                raise ValueError("The code must define a main function")
            # Execute the main function and get the result
            result_df = exec_locals["main"](df)
            # result_df is Dict[str,DataFrame] or DataFrame

            if isinstance(result_df, dict):
                if not all(isinstance(df, pd.DataFrame) for df in result_df.values()):
                    raise TypeError("When returning a dictionary, all values must be DataFrame type")
                # Batch write multiple worksheets
                for sheet_name, df in result_df.items():
                    self.write_data(
                        df,
                        self.get_file_path(result_file_path),
                        sheet_name=sheet_name,
                        **kwargs,
                    )
            elif isinstance(result_df, pd.DataFrame):
                # Maintain original single-table writing logic
                self.write_data(
                    result_df,
                    self.get_file_path(result_file_path),
                    sheet_name=result_sheet_name or sheet_name,
                    **kwargs,
                )
            else:
                raise TypeError("Main function must return DataFrame or Dict[str,DataFrame] type")
            return "Execution completed " + result_file_path
        except Exception as e:
            logger.error(f"Error running code: {e}")
            return f"Error: {str(e)}"

    def run_code_only_log(self, filepath: str, python_code: str, **kwargs) -> str:
        """Execute Python code to process data and log results
        Args:
            filepath: Input file path
            python_code: Python code to execute
            **kwargs: Additional parameters
        Returns:
            Execution result information
        """
        import io
        import sys
        from contextlib import redirect_stdout

        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, **kwargs)

            # Create StringIO object to capture standard output
            output_buffer = io.StringIO()

            # Prepare execution environment with common libraries
            import pandas as pd
            import matplotlib.pyplot as plt
            from io import BytesIO
            import base64
            
            exec_locals = {
                'df': df,
                'pd': pd,
                'plt': plt,
                'BytesIO': BytesIO,
                'base64': base64,
                'filepath': full_path
            }

            # Redirect standard output and execute Python code
            with redirect_stdout(output_buffer):
                run_python_code(python_code, exec_locals)

                if "main" not in exec_locals:
                    raise ValueError("The code must define a main function")

                # Execute the main function and get the result
                result_df = exec_locals["main"](df)

            # Get the captured output
            captured_output = output_buffer.getvalue()
            return f"{captured_output}\n{result_df}"

        except Exception as e:
            logger.error(f"Error running code: {e}")
            return f"Error: {str(e)}"

    def run_code_with_plot(
        self, filepath: str, python_code: str, save_path: str, **kwargs
    ) -> str:
        """Execute Python code with matplotlib plotting functionality
        Args:
            filepath: Input file path
            python_code: Python code to execute
            save_path: Chart save path, if not provided then return base64 encoded image
            **kwargs: Additional parameters
        Returns:
            Execution result information and chart data
        """
        import io
        from contextlib import redirect_stdout
        import matplotlib.pyplot as plt
        import matplotlib as mpl

        # Set Chinese font: sudo apt install fonts-wqy-zenhei
        mpl.rcParams["font.sans-serif"] = [
            "PingFang SC",
            "WenQuanYi Zen Hei",
            "Microsoft YaHei",
            "Arial Unicode MS",
        ]
        mpl.rcParams["axes.unicode_minus"] = False  # Solve negative sign display issue

        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, **kwargs)

            # Create StringIO object to capture standard output
            output_buffer = io.StringIO()

            exec_locals = {"df": df, "pd": pd, "plt": plt}

            # Redirect standard output and execute Python code
            with redirect_stdout(output_buffer):
                run_python_code(python_code, exec_locals)

                if "main" not in exec_locals:
                    raise ValueError("The code must define a main function")

                # Execute the main function
                exec_locals["main"](df, plt)

            # Get the captured output
            captured_output = output_buffer.getvalue()
            print(captured_output)

            # Ensure target directory exists
            save_full_path = self.get_file_path(save_path)
            os.makedirs(os.path.dirname(save_full_path), exist_ok=True)
            # Save chart to file
            plt.savefig(save_full_path)
            plt.close()
            return f"{captured_output}\nChart has been saved to: {save_path}"

        except Exception as e:
            logger.error(f"Error running code with plot: {e}")
            return f"Error: {str(e)}"
        finally:
            plt.close("all")

    def run_code_with_pyecharts(
        self, filepath: str, python_code: str, save_path: str, theme: str = "light", title: str = None, **kwargs
    ) -> str:
        """Execute Python code with pyecharts plotting functionality for a single chart.

        Args:
            filepath: Input file path
            python_code: Python code to execute, defined as def main(df), returns a single pyecharts chart object
            save_path: Chart save path, must end with .html
            theme: Pyecharts theme (e.g., 'light', 'dark', 'chalk', 'vintage'), defaults to 'light'
            title: Optional title for the chart, displayed in the HTML page
            **kwargs: Additional parameters passed to read_data

        Returns:
            str: Execution result information and chart data

        Raises:
            ValueError: If save_path does not end with .html, code lacks main function, or includes invalid imports
            FileNotFoundError: If input file does not exist
            TypeError: If the return value is not a single pyecharts chart object
        """
        import io
        from contextlib import redirect_stdout
        from pyecharts.charts import Page, Grid, Tab
        from pyecharts.globals import ThemeType
        from pyecharts.options import TitleOpts
        import re
        import os

        # Validate save_path
        if not save_path.lower().endswith(".html"):
            raise ValueError("Save path must end with .html")

        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, **kwargs)

            # Create StringIO object to capture standard output
            output_buffer = io.StringIO()

            # Prepare execution environment
            import pandas as pd
            exec_locals = {"df": df, "pd": pd}

            # Redirect standard output and execute Python code
            with redirect_stdout(output_buffer):
                run_python_code(python_code, exec_locals)

                if "main" not in exec_locals:
                    raise ValueError("The code must define a main function")

                # Execute the main function and get the chart object
                chart = exec_locals["main"](df)

            # Validate the returned object (must be a single chart, not a layout)
            if isinstance(chart, (Page, Grid, Tab)):
                raise TypeError("Main function must return a single pyecharts chart object, not a Page, Grid, or Tab")

            # Log chart type for debugging
            logger.debug(f"Chart type: {type(chart).__name__}")

            # Extract chart title safely
            chart_title = 'Chart'
            if hasattr(chart, 'options') and chart.options.get('title'):
                title_opt = chart.options['title']
                if isinstance(title_opt, TitleOpts):
                    # Pyecharts 2.x: Use 'title' attribute
                    chart_title = getattr(title_opt, 'title', 'Chart') or 'Chart'
                elif isinstance(title_opt, list) and title_opt:
                    # Pyecharts 1.x: Title is a list of dictionaries
                    chart_title = title_opt[0].get('text', 'Chart')
                elif isinstance(title_opt, dict):
                    # Handle case where title is a single dictionary
                    chart_title = title_opt.get('text', 'Chart')
                logger.debug(f"Extracted chart title: {chart_title}")

            # Wrap the chart in a Tab layout to align with dashboard behavior
            tab_layout = Tab()
            tab_layout.add(chart, chart_title)

            # Ensure output directory exists
            output_dir = os.path.join(os.getcwd(), "output")
            os.makedirs(output_dir, exist_ok=True)

            # Construct paths relative to output folder
            save_full_path = os.path.join(output_dir, os.path.basename(save_path))
            temp_html_path = os.path.join(output_dir, "temp_" + os.path.basename(save_path))

            # Apply theme and title, then render HTML to temporary file
            valid_themes = ['light', 'dark', 'chalk', 'essos', 'infographic', 'macarons', 'purple-passion', 'roma', 'shine', 'vintage', 'walden', 'westeros', 'wonderland', 'halloween']
            tab_layout.theme = theme if theme in valid_themes else ThemeType.LIGHT
            if title:
                tab_layout.page_title = title
            tab_layout.render(temp_html_path)

            # Read the generated HTML
            with open(temp_html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Define enhanced tab CSS (aligned with dashboard)
            chart_css = """
            .tab {
                display: flex;
                justify-content: center;
                background-color: #ffffff;
                border-bottom: 2px solid #e0e0e0;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                margin-bottom: 20px;
            }
            .tab button {
                background-color: #e0e0e0;
                border: none;
                outline: none;
                cursor: pointer;
                padding: 12px 24px;
                font-size: 16px;
                font-weight: 500;
                color: #333333;
                transition: all 0.3s ease;
                border-radius: 8px 8px 0 0;
                margin: 0 4px;
            }
            .tab button:hover {
                background-color: #d0d0d0;
                color: #007bff;
                transform: translateY(-2px);
            }
            .tab button.active {
                background-color: #007bff;
                color: #ffffff;
                font-weight: 600;
                box-shadow: 0 -2px 4px rgba(0, 0, 0, 0.2);
            }
            .chart-container {
                display: flex;
                justify-content: center;
                align-items: center;
                width: 100%;
                padding: 20px;
                background-color: #ffffff;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            .chart-container .chart-title {
                position: relative;
                margin-left: 10px;
                margin-top: 10px;
                width: fit-content;
                z-index: 10;
            }
            .chart-container canvas {
                width: 100% !important;
                max-width: 100%;
            }
            .chart-container .datazoom-slider {
                display: none !important;
            }
            """

            # Replace or append tab CSS
            if re.search(r'\.tab\s*{', html_content):
                html_content = re.sub(
                    r'(\.tab\s*{[^}]*}\s*\.tab\s*button\s*{[^}]*}\s*\.tab\s*button:hover\s*{[^}]*}\s*\.tab\s*button\.active\s*{[^}]*})',
                    chart_css,
                    html_content,
                    count=1
                )
            else:
                html_content = re.sub(
                    r'(<style>.*?</style>)',
                    r'\1\n<style>\n' + chart_css + '\n</style>',
                    html_content,
                    count=1,
                    flags=re.DOTALL
                )

            # Append JavaScript to activate the first tab by default
            activate_first_tab_js = """
            <script>
            document.addEventListener("DOMContentLoaded", function() {
                var firstTab = document.getElementsByClassName("tablinks")[0];
                if (firstTab) {
                    firstTab.className += " active";
                    var tabName = firstTab.getAttribute("onclick").match(/'([^']+)'/)[1];
                    document.getElementById(tabName).style.display = "block";
                }
            });
            </script>
            """
            html_content = re.sub(
                r'</body>',
                f'{activate_first_tab_js}\n</body>',
                html_content,
                count=1
            )

            # Write the modified HTML to the final file
            with open(save_full_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            # Clean up temporary file
            if os.path.exists(temp_html_path):
                os.remove(temp_html_path)

            # Get the captured output
            captured_output = output_buffer.getvalue()
            return f"{captured_output}\nChart has been saved to: {save_path}"

        except Exception as e:
            logger.error(f"Error generating Pyecharts chart: {e}")
            return f"Error: {str(e)}"

    def run_code_with_pyecharts_dashboard(
        self, filepath: str, python_code: str, save_path: str, theme: str = "light", title: str = None, **kwargs
    ) -> str:
        """Execute Python code to generate a Pyecharts dashboard with multiple charts, each in its own tab.
        
        Args:
            filepath: Input file path
            python_code: Python code to execute, defined as def main(df), returns a Pyecharts layout object (Page, Grid, or Tab)
            save_path: Path to save the dashboard relative to the output folder (e.g., 'dashboard.html')
            theme: Pyecharts theme (e.g., 'light', 'dark', 'chalk', 'vintage'), defaults to 'light'
            title: Optional title for the dashboard, displayed in the HTML page
            **kwargs: Additional parameters passed to read_data
            
        Returns:
            str: Execution result information, including the generated HTML file path
            
        Raises:
            ValueError: If save_path does not end with .html, code lacks main function, or returns invalid type
            FileNotFoundError: If input file does not exist
            TypeError: If the return value is not a Pyecharts Page, Grid, or Tab object
        """
        import io
        from contextlib import redirect_stdout
        from pyecharts.charts import Page, Grid, Tab
        from pyecharts.globals import ThemeType
        import re
        import os

        # Validate save_path
        if not save_path.lower().endswith(".html"):
            raise ValueError("Save path must end with .html")

        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, **kwargs)

            # Create StringIO object to capture standard output
            output_buffer = io.StringIO()

            # Prepare execution environment
            import pandas as pd
            exec_locals = {"df": df, "pd": pd}

            # Redirect standard output and execute Python code
            with redirect_stdout(output_buffer):
                run_python_code(python_code, exec_locals)

                if "main" not in exec_locals:
                    raise ValueError("The code must define a main function")

                # Execute the main function and get the layout object
                layout = exec_locals["main"](df)

            # Validate the returned object
            if not isinstance(layout, (Page, Grid, Tab)):
                raise TypeError("Main function must return a Pyecharts Page, Grid, or Tab object")

            # Log layout type for debugging
            logger.debug(f"Layout type: {type(layout).__name__}")

            # Convert to Tab layout to ensure one chart per tab
            tab_layout = Tab()
            
            # Try to extract components from _components or _charts
            components = getattr(layout, '_components', []) or getattr(layout, '_charts', [])
            logger.debug(f"Found {len(components)} components in layout")

            if components:
                # Handle components: Each becomes a separate tab
                for idx, chart in enumerate(components):
                    try:
                        # Extract chart title from options
                        chart_title = chart.options.get('title', [{}])[0].get('text', f'Chart {idx + 1}')
                        tab_layout.add(chart, chart_title)
                        logger.debug(f"Added chart {idx} with title: {chart_title}")
                    except Exception as e:
                        logger.warning(f"Error processing chart {idx}: {e}")
                        tab_layout.add(chart, f'Chart {idx + 1}')
            else:
                # Fallback: Add the entire layout as a single tab
                try:
                    chart_title = layout.options.get('title', [{}])[0].get('text', 'Dashboard') if hasattr(layout, 'options') else 'Dashboard'
                    tab_layout.add(layout, chart_title)
                    logger.debug(f"Added entire layout as single tab with title: {chart_title}")
                except Exception as e:
                    logger.warning(f"Error processing layout as single tab: {e}")
                    tab_layout.add(layout, 'Dashboard')

            # Validate that at least one chart was added
            if not getattr(tab_layout, '_components', []) and not getattr(tab_layout, '_charts', []):
                logger.error("No valid charts added to Tab layout")
                raise ValueError("No valid charts found in the layout")

            # Ensure output directory exists
            output_dir = os.path.join(os.getcwd(), "output")
            os.makedirs(output_dir, exist_ok=True)

            # Construct paths relative to output folder
            save_full_path = os.path.join(output_dir, os.path.basename(save_path))
            temp_html_path = os.path.join(output_dir, "temp_" + os.path.basename(save_path))

            # Apply theme and title, then render HTML to temporary file
            valid_themes = ['light', 'dark', 'chalk', 'essos', 'infographic', 'macarons', 'purple-passion', 'roma', 'shine', 'vintage', 'walden', 'westeros', 'wonderland', 'halloween']
            tab_layout.theme = theme if theme in valid_themes else ThemeType.LIGHT
            if title:
                tab_layout.page_title = title
            tab_layout.render(temp_html_path)

            # Read the generated HTML
            with open(temp_html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Define enhanced tab CSS
            new_tab_css = """
            .tab {
                display: flex;
                justify-content: center;
                background-color: #ffffff;
                border-bottom: 2px solid #e0e0e0;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                margin-bottom: 20px;
            }
            .tab button {
                background-color: #e0e0e0;
                border: none;
                outline: none;
                cursor: pointer;
                padding: 12px 24px;
                font-size: 16px;
                font-weight: 500;
                color: #333333;
                transition: all 0.3s ease;
                border-radius: 8px 8px 0 0;
                margin: 0 4px;
            }
            .tab button:hover {
                background-color: #d0d0d0;
                color: #007bff;
                transform: translateY(-2px);
            }
            .tab button.active {
                background-color: #007bff;
                color: #ffffff;
                font-weight: 600;
                box-shadow: 0 -2px 4px rgba(0, 0, 0, 0.2);
            }
            .chart-container .chart-title {
                position: relative;  /* Position relative to chart container */
                margin-left: 10px;  /* Offset from left */
                margin-top: 10px;   /* Offset from top */
                width: fit-content; /* Size to content */
                z-index: 10;        /* Above chart elements */
            }
            .chart-container canvas {
                width: 100% !important;  /* Ensure canvas uses full container width */
                max-width: 100%;         /* Prevent overflow */
            }
            .chart-container .datazoom-slider {
                display: none !important; /* Hide data zoom slider if present */
            }
            """

            # Replace or append tab CSS
            if re.search(r'\.tab\s*{', html_content):
                html_content = re.sub(
                    r'(\.tab\s*{[^}]*}\s*\.tab\s*button\s*{[^}]*}\s*\.tab\s*button:hover\s*{[^}]*}\s*\.tab\s*button\.active\s*{[^}]*})',
                    new_tab_css,
                    html_content,
                    count=1
                )
            else:
                html_content = re.sub(
                    r'(<style>.*?</style>)',
                    r'\1\n<style>\n' + new_tab_css + '\n</style>',
                    html_content,
                    count=1,
                    flags=re.DOTALL
                )

            # Append JavaScript to activate the first tab by default
            activate_first_tab_js = """
            <script>
            document.addEventListener("DOMContentLoaded", function() {
                var firstTab = document.getElementsByClassName("tablinks")[0];
                if (firstTab) {
                    firstTab.className += " active";
                    var tabName = firstTab.getAttribute("onclick").match(/'([^']+)'/)[1];
                    document.getElementById(tabName).style.display = "block";
                }
            });
            </script>
            """
            html_content = re.sub(
                r'</body>',
                f'{activate_first_tab_js}\n</body>',
                html_content,
                count=1
            )

            # Write the modified HTML to the final file
            with open(save_full_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            # Clean up temporary file
            if os.path.exists(temp_html_path):
                os.remove(temp_html_path)

            # Get the captured output
            captured_output = output_buffer.getvalue()
            return f"{captured_output}\nDashboard has been saved to: {save_path}"

        except Exception as e:
            logger.error(f"Error generating Pyecharts dashboard: {e}")
            return f"Error: {str(e)}"

    def get_column_correlation(
        self,
        df: pd.DataFrame,
        method: str = "pearson",
        min_correlation: float = 0.5,
        handle_na: str = "drop"
    ) -> str:
        """Calculate correlation between numeric columns in DataFrame.

        Args:
            df: Input DataFrame
            method: Correlation calculation method, supports 'pearson', 'spearman', 'kendall'
            min_correlation: Correlation coefficient threshold, only returns results with absolute 
                            correlation coefficient greater than this value
            handle_na: Strategy for handling missing values, supports 'drop' (remove rows with NaN) 
                    or 'impute' (fill NaN with column means)

        Returns:
            str: Detailed results string containing column correlations, including any warnings about missing values

        Raises:
            ValueError: If invalid method, handle_na option, or insufficient valid data
            Exception: For other unexpected errors during correlation calculation
        """
        try:
            # Validate correlation method
            valid_methods = ["pearson", "spearman", "kendall"]
            if method not in valid_methods:
                raise ValueError(f"Invalid correlation method: {method}. Supported methods: {valid_methods}")

            # Get columns of numeric type
            numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
            if len(numeric_cols) < 2:
                return "Error: Not enough numeric columns to calculate correlation (minimum 2 required)"

            # Check for missing values
            result_prefix = ""
            if df[numeric_cols].isna().any().any():
                na_counts = df[numeric_cols].isna().sum()
                na_report = "\n".join([f"Column '{col}' has {count} missing values" for col, count in na_counts.items() if count > 0])
                if handle_na == "drop":
                    df = df[numeric_cols].dropna()
                    result_prefix = f"Warning: Missing values found:\n{na_report}\nDropped rows with missing values.\n"
                elif handle_na == "impute":
                    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
                    result_prefix = f"Warning: Missing values found:\n{na_report}\nImputed missing values with column means.\n"
                else:
                    raise ValueError(f"Invalid handle_na option: {handle_na}. Supported options: 'drop', 'impute'")

            # Check if enough data remains after handling NaN
            if len(df) < 2:
                return f"Error: Not enough valid data rows ({len(df)}) to calculate correlations after handling missing values"

            # Calculate correlation matrix
            correlation_matrix = df[numeric_cols].corr(method=method)

            # Filter significant correlations
            significant_correlations = []
            for i in range(len(numeric_cols)):
                for j in range(i + 1, len(numeric_cols)):
                    corr = correlation_matrix.iloc[i, j]
                    if abs(corr) >= min_correlation:
                        significant_correlations.append(
                            (f"Correlation coefficient between {numeric_cols[i]} and {numeric_cols[j]} is: {corr:.4f}", corr)
                        )

            # Sort correlations by absolute value (descending)
            significant_correlations.sort(key=lambda x: abs(x[1]), reverse=True)
            significant_correlations = [item[0] for item in significant_correlations]

            if not significant_correlations:
                return f"{result_prefix}No column pairs found with absolute correlation coefficient greater than {min_correlation}"

            return f"{result_prefix}\n".join(significant_correlations)

        except ValueError as e:
            logger.error(f"ValueError in correlation calculation: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error in correlation calculation: {e}")
            return f"Error: Unexpected error during correlation calculation: {str(e)}"
        
    def inspect_data(
        self, filepath: str, preview_rows: int = 5, preview_type: str = "head", **kwargs
    ) -> str:
        """View basic information about the data
        Args:
            filepath: File path
            preview_rows: Number of preview rows
            preview_type: Preview type
            **kwargs: Additional parameters
        Returns:
            String description of data information
        """
        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, **kwargs)
            result = []
            # Data preview
            result.append("=== Data Preview ===")
            preview = (
                df.head(preview_rows)
                if preview_type == "head"
                else df.tail(preview_rows)
            )
            result.append(str(preview))
            # Basic data information
            result.append("\n=== Basic Data Information ===")
            result.append(f"Rows: {df.shape[0]}")
            result.append(f"Columns: {df.shape[1]}")
            result.append(f"Column names: {list(df.columns)}")
            # Data type information
            result.append("\n=== Data Type Information ===")
            result.append(str(df.dtypes))
            # Statistical summary
            result.append("\n=== Statistical Summary ===")
            result.append(str(df.describe()))
            return "\n".join(result)
        except Exception as e:
            logger.error(f"Error inspecting data: {e}")
            return f"Error: {str(e)}"

    def get_missing_values_info(self, df: pd.DataFrame) -> str:
        """Get missing value information

        Args:
            df: DataFrame

        Returns:
            DataFrame containing missing value information
        """
        missing_count = df.isnull().sum()
        missing_percent = (missing_count / len(df) * 100).round(4)

        missing_info = pd.DataFrame(
            {"Missing Value Count": missing_count, "Missing Rate(%)": missing_percent}
        )

        return missing_info.sort_values("Missing Value Count", ascending=False).to_string()

    def get_data_unique_values(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        max_unique: int = 10,
    ) -> str:
        """Get unique value information for specified columns

        Args:
            df: DataFrame
            columns: Columns to check, default is all columns
            max_unique: Maximum number of unique values to display, for columns with unique values 
                        exceeding this number, only show the count

        Returns:
            Dictionary containing unique value information
        """
        result = {}
        cols_to_check = columns if columns else df.columns

        for col in cols_to_check:
            if col in df.columns:
                unique_values = df[col].dropna().unique()
                unique_count = len(unique_values)

                values_list = (
                    unique_values.tolist()
                    if hasattr(unique_values, "tolist")
                    else list(unique_values)
                )
                result[col] = {
                    "count": unique_count,
                    "values": (
                        values_list[:max_unique]
                        if unique_count > max_unique
                        else values_list
                    ),
                    "message": (
                        f"More than {max_unique} unique values, only showing first {max_unique}"
                        if unique_count > max_unique
                        else ""
                    ),
                }

        return str(result)

    def get_random_sample(
        self, df: pd.DataFrame, sample_size: int, **kwargs
    ) -> pd.DataFrame:
        """Get random sample from data

        Args:
            filepath: Input file path
            sample_size: Number of rows to sample
            **kwargs: Additional parameters, will be passed to read_data method

        Returns:
            pd.DataFrame: DataFrame containing random sample data

        Raises:
            ValueError: Raised when sample size is greater than dataset size
        """
        try:
            if sample_size > len(df):
                raise ValueError(
                    f"Sample size ({sample_size}) cannot be greater than dataset size ({len(df)})"
                )

            if sample_size > 20:
                return ValueError(f"Sample size ({sample_size}) greater than 20, random sampling not supported")
            return df.sample(n=sample_size, random_state=None)
        except Exception as e:
            logger.error(f"Error getting random sample: {e}")
            raise
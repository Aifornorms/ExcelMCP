import ast


def transform_top_level_imports(code_string):
    """
    Convert only top-level import statements to __import__ form,
    while preserving imports inside functions
    """
    tree = ast.parse(code_string)
    new_lines = []
    
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            # Skip imports inside functions
            if any(isinstance(parent, ast.FunctionDef) for parent in ast.walk(node)):
                new_lines.append(ast.unparse(node))
            else:
                # Transform top-level imports
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        module_name = alias.name
                        asname = alias.asname
                        if asname:
                            new_lines.append(f"{asname} = __import__('{module_name}')")
                        else:
                            new_lines.append(f"{module_name} = __import__('{module_name}')")
                elif isinstance(node, ast.ImportFrom):
                    module_name = node.module
                    for alias in node.names:
                        imported_name = alias.name
                        asname = alias.asname
                        if asname:
                            new_lines.append(
                                f"{asname} = __import__('{module_name}', fromlist=['{imported_name}']).{imported_name}"
                            )
                        else:
                            new_lines.append(
                                f"{imported_name} = __import__('{module_name}', fromlist=['{imported_name}']).{imported_name}"
                            )
        else:
            new_lines.append(ast.unparse(node))
    
    return "\n".join(new_lines)

def run_python_code(python_code, exec_locals):
    # Add common data science modules
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    
    # Update exec_locals with these modules
    exec_locals.update({
        'pd': pd,
        'np': np,
        'plt': plt
    })
    
    return exec(transform_top_level_imports(python_code), None, exec_locals)
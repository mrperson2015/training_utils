"""
    This module generates a uniform print statement. Used for development and training.
    This is not intended to pe used in production.
"""


# Standard Library Imports <- This is not required but useful to separate out imports
def print_header(header: str):
    """
    Prints a uniform border around the string submitted. Used primarily for development and training

    Examples:
        print_header("Single Line Header")\n
        print_header("Multi Line Header\\\\n"Line 2")

    Args:
        header: Text you want printed into the header format
    """
    width = 108
    dash = "═" * width
    space = " " * width
    print(f"╔═{dash}═╗")
    for line in header.splitlines():
        print(f"║ {(line + space)[:width]} ║")
    print(f"╚═{dash}═╝")


if __name__ == '__main__':
    print_header("Single Line Header")
    print_header("Multi Line Header\n"
                 "Line 2")

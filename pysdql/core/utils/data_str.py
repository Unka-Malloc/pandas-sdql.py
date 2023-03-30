def remove_suffix(input_string: str, suffix: str) -> str:
    if suffix and input_string.endswith(suffix):
        return input_string[:-len(suffix)]
    return input_string


def remove_prefix(input_string: str, prefix: str) -> str:
    if prefix and input_string.startswith(prefix):
        return input_string[len(prefix):]
    return input_string


def remove_sides(input_string: str, pattern: str) -> str:
    return remove_suffix(remove_prefix(input_string, pattern), pattern)

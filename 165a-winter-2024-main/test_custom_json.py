"""
Test script for custom JSON implementation
"""

from lstore.bufferpool import json_load, json_dump, _stringify_value
from lstore.page_range import json_dumps

def test_stringify():
    """Test the _stringify_value function"""
    test_cases = [
        # Simple values
        (None, "null"),
        (True, "true"),
        (False, "false"),
        (42, "42"),
        (3.14, "3.14"),
        ("hello", "\"hello\""),
        
        # Special characters in strings
        ("line1\nline2", "\"line1\\nline2\""),
        ("tab\ttab", "\"tab\\ttab\""),
        ("quote\"quote", "\"quote\\\"quote\""),
        
        # Lists
        ([], "[]"),
        ([1, 2, 3], "[1,2,3]"),
        ([1, "two", True], "[1,\"two\",true]"),
        
        # Dictionaries
        ({}, "{}"),
        ({"a": 1, "b": 2}, "{\"a\":1,\"b\":2}"),
        ({"nested": {"a": 1, "b": [2, 3]}}, "{\"nested\":{\"a\":1,\"b\":[2,3]}}")
    ]
    
    for value, expected in test_cases:
        result = _stringify_value(value)
        assert result == expected, f"Expected {expected}, got {result}"
    
    print("All stringify tests passed!")

def test_parse_and_stringify():
    """Test parsing and then stringifying gives the original value"""
    test_cases = [
        '{"a": 1, "b": 2}',
        '{"nested": {"a": 1, "b": [2, 3]}}',
        '[1, 2, 3, {"a": "b"}]',
        '"hello\\nworld"',
        'true',
        'false',
        'null',
        '42',
        '3.14'
    ]
    
    for json_str in test_cases:
        # Use StringIO to simulate file operations
        import io
        f = io.StringIO(json_str)
        parsed = json_load(f)
        f2 = io.StringIO()
        json_dump(parsed, f2)
        f2.seek(0)
        result = f2.read()
        
        # Parse both to compare semantically (not just string equality)
        f1 = io.StringIO(json_str)
        original_parsed = json_load(f1)
        f3 = io.StringIO(result)
        result_parsed = json_load(f3)
        
        assert original_parsed == result_parsed, f"Original: {original_parsed}, Result: {result_parsed}"
    
    print("All parse and stringify tests passed!")

def test_json_dumps():
    """Test the json_dumps function from page_range.py"""
    test_data = {"test": "value", "numbers": [1, 2, 3]}
    json_str = json_dumps(test_data)
    expected = '{"test":"value","numbers":[1,2,3]}'
    assert json_str == expected, f"Expected {expected}, got {json_str}"
    print("json_dumps test passed!")

def test_indent():
    """Test indentation in json_dump"""
    test_data = {"a": 1, "b": [2, 3, 4], "c": {"d": 5}}
    import io
    f = io.StringIO()
    json_dump(test_data, f, indent=2)
    f.seek(0)
    result = f.read()
    
    # We don't check exact formatting, just that it contains newlines and spaces
    assert "\n" in result, "Expected newlines in indented output"
    assert "  " in result, "Expected spaces in indented output"
    
    # Make sure it's still valid JSON
    f.seek(0)
    parsed = json_load(f)
    assert parsed == test_data, f"Original: {test_data}, Parsed: {parsed}"
    
    print("Indentation test passed!")

if __name__ == "__main__":
    print("Testing custom JSON implementation...")
    test_stringify()
    test_parse_and_stringify()
    test_json_dumps()
    test_indent()
    print("All tests passed!") 
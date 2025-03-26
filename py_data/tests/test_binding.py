from py_data.binding_tester import binding_tester

def test_binding():
    result = binding_tester()
    assert result == "TO_UPPER", f"Expected 'TO_UPPER', but got {result}"
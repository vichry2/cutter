from py_data import binding_tester

def test_cutter():
    result = binding_tester()
    assert result == "TO_UPPER", f"Expected 'TO_UPPER', but got {result}"
import pytest


from entropylab.cli.main import _safe_run_command


# def test_serve():
#     mock_function = create_autospec(serve_results, return_value=None)
#     # mock serve_results
#     # mocker.patch("serve_results")
#     args = argparse.Namespace
#     args.directory = "."
#     args.port = 12345
#     serve(args)
#     mock_function.assert_called_once()


# _safe_run_command()
def test_safe_run_command_with_no_args():
    _safe_run_command(no_args_func)


def test_safe_run_command_with_one_args():
    _safe_run_command(one_args_func, "yo!")


def test_safe_run_command_with_two_args():
    _safe_run_command(two_args_func, "yo!", "dog")


def test_safe_run_command_that_raises():
    with pytest.raises(SystemExit) as se:
        _safe_run_command(two_args_func_that_raises)
        # safe_run_command(no_args_func, "yo!", "dog")
    assert se.type == SystemExit
    assert se.value.code == -1


def no_args_func() -> None:
    print("Yay!")


def one_args_func(one: str) -> None:
    print("Yay! " + one)


def two_args_func(one: str, two: str) -> None:
    print("Yay! " + one + " " + two)


def two_args_func_that_raises():
    raise RuntimeError("Yay!")


def func_under_test(message: str, bar: str):
    raise RuntimeError
    print(message)

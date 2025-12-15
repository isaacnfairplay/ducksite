from ducksearch import cli


def test_serve_placeholder(capsys):
    cli.main(["serve"])
    captured = capsys.readouterr()
    assert "serve is not yet implemented" in captured.out


def test_lint_placeholder(capsys):
    cli.main(["lint"])
    captured = capsys.readouterr()
    assert "lint is not yet implemented" in captured.out

from tools.patterns_analysis._archive.emit_intradomain_definition import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: emit_intradomain_definition") from exc

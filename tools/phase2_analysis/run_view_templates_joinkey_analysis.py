from tools.patterns_analysis._archive.run_view_templates_joinkey_analysis import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: run_view_templates_joinkey_analysis") from exc

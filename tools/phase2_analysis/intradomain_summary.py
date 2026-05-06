from tools.patterns_analysis._archive.intradomain_summary import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: intradomain_summary") from exc

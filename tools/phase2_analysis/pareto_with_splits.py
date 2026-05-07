from tools.patterns_analysis._archive.pareto_with_splits import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: pareto_with_splits") from exc

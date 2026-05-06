from tools.patterns_analysis._archive.build_reference_standards import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: build_reference_standards") from exc

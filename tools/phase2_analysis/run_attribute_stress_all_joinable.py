from tools.patterns_analysis._archive.run_attribute_stress_all_joinable import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: run_attribute_stress_all_joinable") from exc

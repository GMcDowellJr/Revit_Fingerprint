from tools.patterns_analysis._archive.split_detection_element_level import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: split_detection_element_level") from exc

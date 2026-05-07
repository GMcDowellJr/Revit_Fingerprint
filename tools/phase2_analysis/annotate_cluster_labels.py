from tools.patterns_analysis._archive.annotate_cluster_labels import *  # noqa: F401,F403


if __name__ == "__main__":
    try:
        main()
    except NameError as exc:
        raise SystemExit("No main() available in archived module: annotate_cluster_labels") from exc

import tbl_to_csv

if __name__ == "__main__":
    import cProfile, pstats
    profiler = cProfile.Profile()
    profiler.enable()

    input_dir = r"D:\Clusterix\tpch\100G\tbl_x8"
    output_dir = r"D:\Clusterix\tpch\100G\csv_x8"

    converter = tbl_to_csv.TblToCsvConverter(input_dir, output_dir, 2)
    converter.Run()

    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats('tottime')
    stats.print_stats()
    
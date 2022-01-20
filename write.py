def to_files(df, tgt_dir, tgt_file_format):
    df.coalesce(16). \
        write. \
        partitionBy('year', 'month', 'day').\
        mode('append').\
        format(tgt_file_format).\
        save(tgt_dir)


import csv
import os

EVAL_SEGMENT_FN = "segments/eval_segments.csv"
BALANCED_TRAIN_SEGMENT_FN = "segments/balanced_train_segments.csv"
UNBALANCED_TRAIN_SEGMENT_FN = "segments/unbalanced_train_segments.csv"


def make_filtered_segments(filter_set=("/m/09x0r",), combine=True, out_name=""):
    """ Ingest Google-provided segment CSVs, remove entries that do not possess a label in filter_set,
    then save the filtered CSV. Default filter_set includes speech only.

    Args:
        filter_set:     iterable containing names of classes to keep. class names are the funky ones defined in
                        ontology.json
        combine:        Boolean. If True, (eval, unbalanced_train, balanced_train) are all filtered and combined into
                        the same CSV. False is currently not supported.
        out_name:       Name of output CSV file.

    Returns:
        Full path to ${out_name}.
    """

    def write_helper(segment_file, csv_writer, filters):
        with open(segment_file, 'r') as f:
            for row_idx, row in enumerate(csv.reader(f)):
                # Skip commented lines
                if row[0][0] == '#':
                    continue

                # nasty quotations in my way
                row = [entry.replace('"', '') for entry in row]

                # Skip rows that don't contain a label in filter_set
                b = []
                for filt in filters:
                    b.append(any([filt in entry for entry in row]))
                if not any(b):
                    continue

                csv_writer.writerow(row)
        return

    if os.path.exists(out_name):
        raise FileExistsError("I refuse to overwrite %s" % out_name)

    # make sure output file ends with ".csv".
    out_name += ".csv" if not out_name.endswith(".csv") else ""

    # open output csv.
    with open(out_name, 'w+') as csv_file:
        writer = csv.writer(csv_file)
        # call function that actually checks filters and writes rows.
        write_helper(EVAL_SEGMENT_FN, writer, filter_set)
        write_helper(BALANCED_TRAIN_SEGMENT_FN, writer, filter_set)
        write_helper(UNBALANCED_TRAIN_SEGMENT_FN, writer, filter_set)


if __name__ == "__main__":
    # path works assuming you run this from the project root.
    make_filtered_segments(out_name="./segments/speech_segments.csv")

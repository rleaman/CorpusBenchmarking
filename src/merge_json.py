import sys
import json

def merge_json_files():
    # sys.argv[0] is the script name, so we slice from [1:]
    args = sys.argv[1:]

    # Validation: We need pairs (label + file) and one final output filename.
    # Total args must be odd and at least 3.
    if len(args) < 3 or len(args) % 2 == 0:
        print("Usage: python merge_json.py <label1> <file1> [<label2> <file2> ...] <output_file>")
        sys.exit(1)

    # The last argument is our destination
    output_filename = args[-1]
    # All arguments before the last one are the pairs
    pair_args = args[:-1]

    output_dict = {}

    # Iterate through pairs using a step of 2
    for i in range(0, len(pair_args), 2):
        label = pair_args[i]
        json_file = pair_args[i+1]

        try:
            with open(json_file, 'r') as f:
                content = json.load(f)
                output_dict[label] = content
                print(f"Successfully loaded '{json_file}' into label '{label}'.")
        except FileNotFoundError:
            print(f"Error: File '{json_file}' not found. Skipping.")
        except json.JSONDecodeError:
            print(f"Error: Could not parse '{json_file}' as JSON. Skipping.")

    # Write the final dictionary to the output file
    with open(output_filename, 'w') as out_f:
        json.dump(output_dict, out_f, indent=4)
        print(f"\nFinal merged JSON saved to: {output_filename}")

if __name__ == "__main__":
    merge_json_files()

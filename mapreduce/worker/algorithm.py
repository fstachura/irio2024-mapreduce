import re
import string

def map_function(input_file, output_file, input_file_start, input_file_end):
    with input_file.open('rb') as r, output_file.open('w') as w:
        r.seek(input_file_start)
        data = r.read(input_file_end - input_file_start).decode()

        rgx = "(?:\\s|" + "|".join("\\" + c for c in string.punctuation) + ")+"
        for word in filter(len, re.split(rgx, data)):
            w.write(f"{word},1\n")

def reduce_function(input_file, output_file, input_file_start, input_file_end):
    word_cnt = {}
    with input_file.open('rb') as r:
        r.seek(input_file_start)
        for line in r.read(input_file_end - input_file_start).decode().strip().split('\n'):
            word, cnt = line.split(',')
            word_cnt[word] = word_cnt.get(word, 0) + int(cnt)

    with output_file.open('w') as w:
        for word, cnt in word_cnt.items():
            w.write(f"{word},{cnt}\n")

STEP_ID_TO_FUNCTION = {
    "map": map_function,
    "reduce": reduce_function
}
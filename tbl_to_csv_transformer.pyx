
class TblToCsvTransformer:
    def transform_file_line(self, line: str) -> str:
        line_parts = line.split('|')
        '''line_parts_count = len(line_parts) - 2
        line_parts = line_parts[0:line_parts_count + 1]
        with io.StringIO() as output:
            for i, part in enumerate(line_parts):
                output.write('"')
                output.write(part)
                output.write('"')
                if (i < line_parts_count):
                    output.write(",")
                else:
                    output.write("\n")
            return output.getvalue()'''

        line_parts = line_parts[0:len(line_parts)-1]
        return ",".join(f'"{str}"' for str in line_parts) + "\n"

    def transform_file(self, input_file_path: str, ouput_file_path: str, header: str):
        msg = input_file_path + " -> " + ouput_file_path
        print(msg)
        lineCount = 0
        with open(ouput_file_path, 'w', buffering=1024*1024*4) as outfile, open(input_file_path, 'r', encoding='utf-8', buffering=1024*1024*4) as infile:
            outfile.write(header)
            outfile.write("\n")
            for line in infile:
                outfile.write(self.transform_file_line(line))
                lineCount += 1
                if (lineCount % 1000000 == 0):
                    print(msg + ": " + str(lineCount))

# input should be words and line_width. return will be list of line rects based on best assumption
def get_text_lines(words, line_width):
    # for our use case, the x axis is fixed
    line_left = None
    current_line_top = None

    lines = list()
    for word_idx, word in enumerate(words):    
        current_word_top = round(word["top"], 3)
        current_word_left = round(word["x0"], 3)

        new_line = False
    #     Not initialized
        if not line_left:
            new_line = True
            line_left = current_word_left
            current_line_top = current_word_top
        else:
            if abs(line_left - current_word_left) < 10:
                if abs(current_line_top - current_word_top) > 10:
                    new_line = True

        if abs(current_line_top - current_word_top) > 10:
            if abs(line_left - current_word_left) > 10:
                current_line_top = current_word_top
            new_line = True

        is_last_word = True if word_idx == len(words) - 1 else False

        if new_line or is_last_word:
            if new_line:
                current_line_top = current_word_top
                obj = {"x0" : line_left, "top" : word["top"]}
                lines.append(obj)

            last_word = None        
            if word_idx != 0:
                last_word = words[word_idx-1]

            if is_last_word:
                last_word = word

            if last_word:
                if new_line:
                    last_line = lines[-2] if len(lines) > 1 else lines[-1]
                else:
                    last_line = lines[-1]
                last_line["x1"] = line_width
                last_line["bottom"] = last_word["bottom"]
    return lines

# Following function does not really work. it needs fixing, but we could get away by using parse_pdf_page
def get_bounding_rects(index, words, tables, line_width):
    table_boxes = []
    for table in tables:
        table_box = Box(*table.bbox)
        table_boxes.append(table_box)

    content = dict()
    line_left = None
    current_line_top = None
    current_table_index = None

    # We are assuming that we have lines and tables in vertical manner. there will not be any words outside of the table.
    for word_idx, word in enumerate(words):
        current_word_top = round(word["top"], 3)
        current_word_left = round(word["x0"], 3)
        
        is_line = True
        word_box = Box.from_word(word)
        table_index = None
        for idx, t_box in enumerate(table_boxes):
            if t_box.is_overlapped(word_box):
                is_line= False
                table_index = idx
                
        if is_line:
            current_word_top = round(word["top"], 3)
            current_word_left = round(word["x0"], 3)

            new_line = False
        #     Not initialized
            if not line_left:
                new_line = True
                line_left = current_word_left
                current_line_top = current_word_top
            elif line_left > current_word_left:
                # we found a word to the left to our line start assumption
                line_left = current_word_left
                new_line = True

            else:
                if abs(line_left - current_word_left) < 10:
                    if abs(current_line_top - current_word_top) > 10:
                        new_line = True

            if abs(current_line_top - current_word_top) > 10:
                if abs(line_left - current_word_left) > 10:
                    current_line_top = current_word_top
                new_line = True

            is_last_word = True if word_idx == len(words) - 1 else False

            if new_line or is_last_word:
                if new_line:
                    index = index + 1
                    current_line_top = current_word_top
                    content[index] = {"type": "line", "data": list()}
                    obj = {"x0" : line_left, "top" : word["top"]}
                    # lines.append(obj)
                    content[index]["data"].append(obj)

                last_word = None        
                if word_idx != 0:
                    last_word = words[word_idx-1]

                if is_last_word:
                    last_word = word

                if last_word:
                    lines = content[index]["data"]
                    if new_line:
                        last_line = lines[-2] if len(lines) > 1 else lines[-1]
                    else:
                        last_line = lines[-1]
                    last_line["x1"] = line_width
                    last_line["bottom"] = last_word["bottom"]
        else:
            if current_table_index == None: 
                current_table_index = table_index
                index = index + 1
                content[index] = {"type": "table", "data": tables[current_table_index]}
            
            if current_table_index != table_index:
                current_table_index = table_index
                index = index + 1
                content[index] = {"type": "table", "data": tables[current_table_index]}

    return content

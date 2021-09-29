def sort_content(content, col_numbers_list, separator):
    # Sort content by the column numbers provided in list
    sorted_content = list(map(lambda x: separator.join(x), sorted(list(map(lambda x: x.split(separator), content)),
                                                                  key=lambda x: (x[0],  # Participation Number
                                                                                 x[1]   # Ordinal Number
                                                                                 ))))

    return sorted_content


if __name__ == '__main__':
    file_path = '../files/data_file_from_azure_bud.txt'
    with open(file_path) as in_fh:
        lines = in_fh.readlines()
    sc = sort_content(lines, [1, 2], '~')
    print(sc[10])

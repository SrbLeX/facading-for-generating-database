import xmlrpc.client
import xlsxwriter


# Instantiating and defining the server proxy instance
proxy = xmlrpc.client.ServerProxy('http://localhost:18000', allow_none=True)


class ExpToExcel:

    def data_to_excel(self, file_name, sheet_name, data):

        # Create a workbook and add a worksheet
        workbook = xlsxwriter.Workbook(file_name + '.xlsx')
        worksheet = workbook.add_worksheet(sheet_name)

        # Define the width of the columns
        last = len(data[0].keys())
        column_width = worksheet.set_column(0, last, 13)

        # Define the format of column names
        header_cell_format = workbook.add_format(
            {'bold': True, 'border': True, 'bg_color': 'yellow'}
        )

        # Defining and formating record columns
        body_cell_format = workbook.add_format({'border': True})

        # Writing "header"- column names on Excel sheet
        row_index = 0
        for column_index, column_name in enumerate(data[0].keys()):
            worksheet.write(row_index, column_index, column_name, column_width)
            worksheet.write(row_index, column_index, column_name, header_cell_format)

        # Printing records on Excel sheet
        for row_index, row in enumerate(data, 1):
            for column_index, column in enumerate(row.values()):
                worksheet.write(row_index, column_index, column, body_cell_format)

        # Closing workbook
        workbook.close()


if __name__ == '__main__':
    # Instantiating 'Generate' class methods:
    all_active_users = proxy.all_active_users()
    # print(f"Printing 'all_act_users' : {all_active_users}")

    all_inactive_users = proxy.all_inactive_users()
    # print(f"\nPrinting 'all_inactive_users' : {all_inactive_users}")

    sub_30d_end_austria_box = proxy.sub_30d_end_austria_box()
    # print(f"\nPrinting 'sub_30d_end_austria_box' : {sub_30d_end_austria_box}")

    all_users = proxy.all_users()
    # print(f"\nPrinting 'all_users' : {all_users}")

    # Instantiating class:
    export_to_excel = ExpToExcel()

    # Exporting and writing data to Excel:

    # task 1
    print(f"\nPrinting 'task 1'- 'all_active_users'"
          f" with {len(all_active_users)} active users: {all_active_users}")
    print("Exporting 'all_active_users' to Excel...")
    export_to_excel.data_to_excel('exercise3.a', 'sheet1', all_active_users)

    # task 2
    print(f"\nPrinting 'task 2'- 'all_inactive_users'"
          f" with {len(all_inactive_users)} inactive users: {all_inactive_users}")
    print("Exporting 'all_inactive_users' to Excel...")
    export_to_excel.data_to_excel('exercise3.b', 'sheet1', all_inactive_users)

    # task 3
    print(f"\nPrinting 'task 3'- 'sub_30d_end_austria_box'"
          f" with {len(sub_30d_end_austria_box)} users "
          f" whose subscription expires in the next month,"
          f" which are from Austria"
          f" and have a 'box' package : {sub_30d_end_austria_box}")
    print("Exporting 'sub_30d_end_austria_box' to Excel...")
    export_to_excel.data_to_excel('exercise3.c', 'sheet1', sub_30d_end_austria_box)

    # task 4
    print(f"\nPrinting 'task 4'- 'all_users'"
          f" with {len(all_users)} users: {all_users}")
    print("Exporting 'all_users' to Excel...")
    export_to_excel.data_to_excel('exercise3.d', 'sheet1', all_users)

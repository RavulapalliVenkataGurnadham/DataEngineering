import streamlit as st
# from main import main
import sys
import os 
import pandas as pd
# from main import main

a = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..'))
sys.path.append(a)
from main import main
# main()
summary = main()
print(summary)
# st.write(summary)
# # import json
# summary = {'total_records': 100000, 'column_list': ['Index', 'Customer Id', 'First Name', 'Last Name', 'Company', 'City', 'Country', 'Phone 1', 'Phone 2', 'Email', 'Subscription Date', 'Website', 'order_id'], 'total_no_columns': 13, 'columns_data_types': [('Index', 'int'), ('Customer Id', 'string'), ('First Name', 'string'), ('Last Name', 'string'), ('Company', 'string'), ('City', 'string'), ('Country', 'string'), ('Phone 1', 'string'), ('Phone 2', 'string'), ('Email', 'string'), ('Subscription Date', 'string'), ('Website', 'string'), ('order_id', 'int')], 'duplicate_count': 0, 'duplicate_percentage': 0.0}

# import streamlit as st
# import pandas as pd
# # Summary data
# summary = {
#     'total_records': 100000,
#     'column_list': ['Index', 'Customer Id', 'First Name', 'Last Name', 'Company', 'City', 'Country', 'Phone 1', 'Phone 2', 'Email', 'Subscription Date', 'Website', 'order_id'],
#     'total_no_columns': 13,
#     'columns_data_types': [
#         ('Index', 'int'), 
#         ('Customer Id', 'string'), 
#         ('First Name', 'string'), 
#         ('Last Name', 'string'), 
#         ('Company', 'string'), 
#         ('City', 'string'), 
#         ('Country', 'string'), 
#         ('Phone 1', 'string'), 
#         ('Phone 2', 'string'), 
#         ('Email', 'string'), 
#         ('Subscription Date', 'string'), 
#         ('Website', 'string'), 
#         ('order_id', 'int')
#     ],
#     'duplicate_count': 0,
#     'duplicate_percentage': 0.0
# }

# Displaying the summary data
st.title("Data Profiling Summary")

# Display total records
st.subheader("Total Records")
st.write(summary['total_records'])

# Display column list
# st.subheader("Column List")
# st.write(summary['column_list'])
formatted_column_list = [f'{i}: "{col}"' for i, col in enumerate(summary['column_list'])]
st.write(formatted_column_list)
# Display total number of columns
st.subheader("Total Number of Columns")
st.write(summary['total_no_columns'])

# Display columns data types
st.subheader("Columns Data Types")
columns_data_types = summary['columns_data_types']
columns_df = pd.DataFrame(columns_data_types, columns=["Column Name", "Data Type"])
st.table(columns_df)

# Display duplicate count
st.subheader("Duplicate Count")
st.write(summary['duplicate_count'])

# Display duplicate percentage
st.subheader("Duplicate Percentage")
st.write(summary['duplicate_percentage'])

# def display_profile_report(profile_report):
#     st.title("Categorical Data Profiling Report")
#     for column_report in profile_report['categorical_columns']:
#         st.subheader(f"Column: {column_report['column_name']}")
#         st.write(f"Total Records: {column_report['total_records']}")
#         st.write(f"Distinct Count: {column_report['distinct_count']}")
#         st.write(f"Zero Count: {column_report['zero_count']}")
#         st.write(f"Null Count: {column_report['null_count']}")
#         st.write(f"Percent Missing: {column_report['percent_missing']:.2f}%")
        
#         st.write("Value Counts:")
#         st.json(column_report['value_counts'])

#         st.write(f"Mode: {column_report['mode']}")
#         st.write("Top Values:")
#         st.json(column_report['top_values'])

# # def save_profile_report(profile_report, output_path):
# #     with open(output_path, 'a') as f:
# #         f.write(json.dumps(profile_report))
# #         f.write('\n')

# #     st.success("Profiling report saved successfully!")






# import streamlit as st

# Title and Text
# st.title("Welcome to my Streamlit App!")
# st.write("This is a simple example of a Streamlit app.")

# # Input Widgets
# name = st.text_input("Enter your name:")
# age = st.number_input("Enter your age:")

# # Display Input Data
# if name:  # Check if name is entered
#     st.write("Hello, {}!".format(name))

# if age:  # Check if age is entered
#     if age >= 18:
#         st.success("You are eligible!")
#     else:
#         st.warning("You are not eligible.")

# # Buttons
# if st.button("Click me!"):
#     st.balloons()  # Fun animation

# # Checkbox and Radio Buttons
# selection = st.checkbox("Select me")
# if selection:
#     st.write("Checkbox is selected.")

# choice = st.radio("Choose an option:", ("Option 1", "Option 2", "Option 3"))
# st.write("You selected:", choice)

# # Side-bar and Markdown
# with st.sidebar:
#     st.subheader("Sidebar Options")
#     uploaded_file = st.file_uploader("Choose a file:")
#     if uploaded_file is not None:
#         bytes_data = uploaded_file.read()
#         st.write("Uploaded file content:", bytes_data.decode("utf-8"))

# st.markdown("---")  # Add a separator

# # Images and Videos
# st.image("image.jpg", width=200)  # Replace "image.jpg" with your image path
# st.video("video.mp4", start_time=0, end_time=10)  # Replace "video.mp4" with your video path

# # Run the app
# if __name__ == "__main__":
#     st.snow()  # Another fun animation
#     st.balloons()  # Can repeat for extra fun
#     st.sidebar.snow()  # Add animations to sidebar too (optional)
#     st.sidebar.balloons()

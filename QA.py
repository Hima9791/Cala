import streamlit as st
import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font
import time
from tqdm import tqdm
from io import BytesIO

def create_key(df):
    df['Key'] = df['ChemicalID'].astype(str) + '_' + df['PartNumber'].astype(str)
    return df

def validate_rows_count(df):
    actual_counts = df['Key'].value_counts().reset_index()
    actual_counts.columns = ['Key', 'ActualRowsCount']
    df = df.merge(actual_counts, on='Key')
    df['RowsCountGap'] = df['RowsCount '] - df['ActualRowsCount']
    df['Automated QA Comment'] = df.apply(
        lambda x: (x['Automated QA Comment'] + ' | ' if x['Automated QA Comment'] else '') + 'Rows count mismatch'
        if x['RowsCountGap'] != 0 else x['Automated QA Comment'], axis=1
    )
    return df

def check_fmd_revision_flag(df):
    df['Automated QA Comment'] = df.apply(
        lambda x: (x['Automated QA Comment'] + ' | ' if x['Automated QA Comment'] else '') + 'FMDRevFlag is Not Latest'
        if x['FMDRevFlag'] == 'Not Latest' else x['Automated QA Comment'], axis=1
    )
    return df

def check_homogeneous_material_mass_variation(df):
    df['HomogeneousMaterialName'] = df['HomogeneousMaterialName'].str.lower()
    grouped = df.groupby(['Key', 'HomogeneousMaterialName'])
    for (key, material_name), group in grouped:
        unique_masses = group['HomogeneousMaterialMass '].nunique()
        if unique_masses > 1:
            df.loc[(df['Key'] == key) & (df['HomogeneousMaterialName'] == material_name), 'Automated QA Comment'] = df.loc[
                (df['Key'] == key) & (df['HomogeneousMaterialName'] == material_name), 'Automated QA Comment'
            ].apply(lambda x: x + ' | Multiple masses for the same homogeneous material' if x else 'Multiple masses for the same homogeneous material')
    return df

def check_homogeneous_material_mass(df):
    df['HomogeneousMaterialName'] = df['HomogeneousMaterialName'].str.lower()
    df['CalculatedMass'] = df.groupby(['Key', 'HomogeneousMaterialName'])['Mass '].transform('sum')
    df['Homogeneous Mass Gap'] = df['CalculatedMass'] - df['HomogeneousMaterialMass ']
    df['MassMismatch'] = df['Homogeneous Mass Gap'].abs() >= 1
    df['Automated QA Comment'] = df.apply(
        lambda x: (x['Automated QA Comment'] + ' | ' if x['Automated QA Comment'] else '') + 'Fail: Mass mismatch'
        if x['MassMismatch'] else x['Automated QA Comment'], axis=1
    )
    return df

def check_substance_homogeneous_material_percentage(df):
    df['homogeneousPercentageSum'] = df.groupby(['Key', 'HomogeneousMaterialName'])['SubstanceHomogeneousMaterialPercentage '].transform('sum')
    df['PercentageMatch'] = (df['homogeneousPercentageSum'] >= 99.9) & (df['homogeneousPercentageSum'] <= 100.10)
    df['PercentageMatchComment'] = df.apply(lambda x: 'Fail: homogeneousPercentage sum != 100' if not x['PercentageMatch'] else '', axis=1)
    df['Automated QA Comment'] = df.apply(lambda x: x['Automated QA Comment'] + ' | ' + x['PercentageMatchComment'] if x['PercentageMatchComment'] else x['Automated QA Comment'], axis=1)
    return df

def check_substance_homogeneous_material_ppm(df):
    df['homogeneousPPMSum'] = df.groupby(['Key', 'HomogeneousMaterialName'])['SubstanceHomogeneousMaterialPercentagePPM '].transform('sum')
    df['PPMMatch'] = (df['homogeneousPPMSum'] >= 999000.0) & (df['homogeneousPPMSum'] <= 1001000.0)
    df['PPMMatchComment'] = df.apply(lambda x: 'Fail: homogeneousPPM sum != 1000000' if not x['PPMMatch'] else '', axis=1)
    df['Automated QA Comment'] = df.apply(lambda x: x['Automated QA Comment'] + ' | ' + x['PPMMatchComment'] if x['PPMMatchComment'] else x['Automated QA Comment'], axis=1)
    return df

def check_substance_component_level_percentage(df):
    df['ComponentPercentageSum'] = df.groupby('Key')['SubstanceComponentLevelPercentage '].transform('sum')
    df['ComponentPercentageMatch'] = (df['ComponentPercentageSum'] >= 99.0) & (df['ComponentPercentageSum'] <= 101.0)
    df['ComponentPercentageMatchComment'] = df.apply(lambda x: 'Fail: Component level percentage sum != 100' if not x['ComponentPercentageMatch'] else '', axis=1)
    df['Automated QA Comment'] = df.apply(lambda x: x['Automated QA Comment'] + ' | ' + x['ComponentPercentageMatchComment'] if x['ComponentPercentageMatchComment'] else x['Automated QA Comment'], axis=1)
    return df

def check_substance_component_level_ppm(df):
    df['ComponentPPMSum'] = df.groupby('Key')['SubstanceComponentLevelPPM '].transform('sum')
    df['ComponentPPMMatch'] = (df['ComponentPPMSum'] >= 990000.0) & (df['ComponentPPMSum'] <= 1010000.0)
    df['ComponentPPMMatchComment'] = df.apply(lambda x: 'Fail: Component level PPM sum != 1000000' if not x['ComponentPPMMatch'] else '', axis=1)
    df['Automated QA Comment'] = df.apply(lambda x: x['Automated QA Comment'] + ' | ' + x['ComponentPPMMatchComment'] if x['ComponentPPMMatchComment'] else x['Automated QA Comment'], axis=1)
    return df

def calculate_gap_and_comment(df):
    gap = (df['TotalComponentMassProfile '] - df['TotalComponentMassSummation ']).abs()
    gap_percentage = (gap / df['TotalComponentMassProfile ']) * 100
    df['Automated QA Comment'] = df.apply(
        lambda x: (x['Automated QA Comment'] + ' | ' if x['Automated QA Comment'] else '') + 'Total VS Summation Gap is more than 50%'
        if (x['TotalComponentMassProfile '] != 0 and (abs(x['TotalComponentMassProfile '] - x['TotalComponentMassSummation '])/x['TotalComponentMassProfile ']*100) >= 50) else x['Automated QA Comment'], axis=1
    )
    return df

def check_total_component_mass_summation(df):
    unique_keys = df['Key'].unique()
    for key in tqdm(unique_keys, desc="Checking Total Component Mass Summation"):
        group = df[df['Key'] == key]
        total_mass_sum = round(group['Mass '].sum(), 4)
        total_component_mass_summation = group['TotalComponentMassSummation '].iloc[0]
        if total_mass_sum != total_component_mass_summation:
            df.loc[df['Key'] == key, 'Automated QA Comment'] = df.loc[df['Key'] == key, 'Automated QA Comment'].apply(
                lambda x: x + ' | Software issue' if pd.notnull(x) and x else 'Software issue'
            )
    return df

def clear_worksheet_but_keep_header(worksheet):
    for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
        for cell in row:
            cell.value = None

def run_all_checks(file_data):
    start_time = time.time()
    # Load workbook from the uploaded file (file_data is a file-like object)
    workbook = openpyxl.load_workbook(file_data)
    worksheet = workbook.active
    data = worksheet.values
    columns = next(data)[0:]
    df = pd.DataFrame(data, columns=columns)
    if 'Automated QA Comment' not in df.columns:
        df['Automated QA Comment'] = ''
    df = create_key(df)
    df = check_fmd_revision_flag(df)
    df = check_homogeneous_material_mass_variation(df)
    df = validate_rows_count(df)
    df = check_homogeneous_material_mass(df)
    df = check_substance_homogeneous_material_percentage(df)
    df = check_substance_homogeneous_material_ppm(df)
    df = check_substance_component_level_percentage(df)
    df = check_substance_component_level_ppm(df)
    df = calculate_gap_and_comment(df)
    df = check_total_component_mass_summation(df)
    added_columns = ['RowsCountGap', 'Homogeneous Mass Gap', 'homogeneousPercentageSum', 'homogeneousPPMSum', 'ComponentPercentageSum', 'ComponentPPMSum', 'Automated QA Comment']
    df = df[list(columns) + added_columns]
    clear_worksheet_but_keep_header(worksheet)
    # Write headers and set font color for added columns
    for c_idx, column in enumerate(df.columns, start=1):
        cell = worksheet.cell(row=1, column=c_idx, value=column)
        if column in added_columns:
            cell.font = Font(color="FF0000")
    # Write data starting from the second row
    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=False), start=2):
        for c_idx, value in enumerate(row, start=1):
            worksheet.cell(row=r_idx, column=c_idx, value=value)
    # Save the updated workbook to a BytesIO buffer
    output_buffer = BytesIO()
    workbook.save(output_buffer)
    output_buffer.seek(0)
    end_time = time.time()
    elapsed_time = end_time - start_time
    mins, secs = divmod(elapsed_time, 60)
    st.write(f"Process Complete. Elapsed Time: {int(mins):02}:{int(secs):02}")
    return output_buffer

def main():
    # Inject custom CSS to hide the Streamlit footer (and menu, if desired)
    hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)
    
    st.title("QA Checker for Chemical Smart Checkers")
    
    # Pop-up question using a radio button
    answer = st.radio("Is Ibrahem a good person?", ("Yes", "No"))
    
    if answer == "No":
        st.warning("ok, check it manually")
    else:
        st.markdown("Upload your Excel file to run the QA checks.")
        uploaded_file = st.file_uploader("Upload an Excel file", type=["xlsx", "xls"])
        if uploaded_file is not None:
            try:
                result_buffer = run_all_checks(uploaded_file)
                st.success("File processed successfully!")
                st.download_button(
                    label="Download Processed File",
                    data=result_buffer,
                    file_name="output_checked.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except Exception as e:
                st.error(f"Error processing file: {e}")

if __name__ == '__main__':
    main()

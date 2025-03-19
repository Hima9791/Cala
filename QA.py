import streamlit as st
import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font
import time
from tqdm import tqdm
from io import BytesIO

# ========================
# --- QA Check Functions ---
# ========================

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

# ========================
# --- Chunk Processing ---
# ========================

def process_chunk(chunk):
    if 'Automated QA Comment' not in chunk.columns:
        chunk['Automated QA Comment'] = ''
    
    chunk = check_fmd_revision_flag(chunk)
    chunk = check_homogeneous_material_mass_variation(chunk)
    chunk = validate_rows_count(chunk)
    chunk = check_homogeneous_material_mass(chunk)
    chunk = check_substance_homogeneous_material_percentage(chunk)
    chunk = check_substance_homogeneous_material_ppm(chunk)
    chunk = check_substance_component_level_percentage(chunk)
    chunk = check_substance_component_level_ppm(chunk)
    chunk = calculate_gap_and_comment(chunk)
    chunk = check_total_component_mass_summation(chunk)
    
    return chunk

def run_all_checks(file_data):
    start_time = time.time()
    # Load the workbook and extract data from the active worksheet
    workbook = openpyxl.load_workbook(file_data)
    worksheet = workbook.active
    data = worksheet.values
    columns = next(data)  # Get header row
    df_all = pd.DataFrame(data, columns=columns)
    
    if 'Automated QA Comment' not in df_all.columns:
        df_all['Automated QA Comment'] = ''
    
    # Create unique key column and get unique keys
    df_all = create_key(df_all)
    unique_keys = df_all['Key'].unique()
    
    chunk_size = 500  # Adjust chunk size as needed
    
    # Define the extra columns added by the QA checks
    added_columns = ['RowsCountGap', 'Homogeneous Mass Gap', 'homogeneousPercentageSum', 
                     'homogeneousPPMSum', 'ComponentPercentageSum', 'ComponentPPMSum', 'Automated QA Comment']
    final_columns = list(columns) + added_columns
    
    # Create a new workbook for the output
    output_workbook = openpyxl.Workbook()
    sheet_out = output_workbook.active
    
    # Write the header row with red font for added columns
    for c_idx, column in enumerate(final_columns, start=1):
        cell = sheet_out.cell(row=1, column=c_idx, value=column)
        if column in added_columns:
            cell.font = Font(color="FF0000")
    
    row_idx = 2
    # Process the data in chunks
    for i in tqdm(range(0, len(unique_keys), chunk_size), desc="Processing Chunks"):
        key_chunk = unique_keys[i:i+chunk_size]
        df_chunk = df_all[df_all['Key'].isin(key_chunk)]
        df_chunk = process_chunk(df_chunk)
        # Ensure all final columns exist in the chunk
        for col in final_columns:
            if col not in df_chunk.columns:
                df_chunk[col] = ''
        df_chunk = df_chunk[final_columns]
        
        for r in dataframe_to_rows(df_chunk, index=False, header=False):
            for c_idx, value in enumerate(r, start=1):
                sheet_out.cell(row=row_idx, column=c_idx, value=value)
            row_idx += 1
    
    output_buffer = BytesIO()
    output_workbook.save(output_buffer)
    output_buffer.seek(0)
    
    end_time = time.time()
    mins, secs = divmod(end_time - start_time, 60)
    st.write(f"Process Complete. Elapsed Time: {int(mins):02}:{int(secs):02}")
    
    return output_buffer

# ========================
# --- Streamlit App UI ---
# ========================

def main():
    # Inject custom CSS to hide certain Streamlit UI elements
    hide_elements = """
    <style>
    #MainMenu {visibility: hidden;}
    footer a[href*="github.com"] {display: none !important;}
    div[class^="_profilePreview"] {display: none !important;}
    </style>
    """
    st.markdown(hide_elements, unsafe_allow_html=True)
    
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

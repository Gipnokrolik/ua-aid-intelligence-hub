import os
import re
import pdfplumber
import pandas as pd


def process_reports_to_master_csv():
    """
    Parses all PDF reports in the raw directory.
    Extracts clean category names by isolating the suffix of the filename.
    """
    # 1. Path Management using relative references
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    input_dir = os.path.join(project_root, "data", "raw", "united24")
    output_path = os.path.join(input_dir, "u24_master_dataset.csv")

    all_records = []

    if not os.path.exists(input_dir):
        print(f"Directory not found: {input_dir}")
        return

    # Filter for PDF files within the target directory
    pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
    print(f"Synchronizing {len(pdf_files)} report files...")

    for filename in pdf_files:
        file_path = os.path.join(input_dir, filename)

        # CATEGORY EXTRACTION LOGIC:
        # Splits 'report-date-health.pdf' by '-' and takes the last part.
        # Then removes the '.pdf' extension to get a clean 'health' label.
        clean_category = os.path.splitext(filename.split('-')[-1])[0].lower()

        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text:
                        continue

                    for line in text.split('\n'):
                        line = line.strip()
                        # Identify records starting with the date pattern (DD.MM.YYYY)
                        date_match = re.search(r'^(\d{2}\.\d{2}\.\d{4})', line)
                        if not date_match:
                            continue

                        date_val = date_match.group(1)
                        content_remainder = line[len(date_val):].strip()

                        # Split by space occurring after the decimal pattern (,XX)
                        # This separates UAH from USD amounts reliably
                        data_segments = re.split(r'(?<=,\d{2})\s+', content_remainder)

                        if len(data_segments) >= 2:
                            try:
                                # Numerical normalization: remove whitespace, convert decimal comma to dot
                                uah_float = float(data_segments[0].replace(' ', '').replace(',', '.'))
                                usd_float = float(data_segments[1].replace(' ', '').replace(',', '.'))

                                all_records.append({
                                    'date': date_val,
                                    'amount_uah': uah_float,
                                    'amount_usd': usd_float,
                                    'fund_name': 'United24',
                                    'category': clean_category
                                })
                            except (ValueError, IndexError):
                                # Skip malformed numeric strings
                                continue
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")

    if all_records:
        # 2. DataFrame Construction & Sorting
        df = pd.DataFrame(all_records)

        # Convert date to datetime objects for accurate chronological sorting
        df['date_dt'] = pd.to_datetime(df['date'], dayfirst=True)
        # Sort by date and then by category
        df = df.sort_values(by=['date_dt', 'category']).drop(columns=['date_dt'])

        # Suppress scientific notation for the final preview
        pd.options.display.float_format = '{:.2f}'.format

        # 3. Export consolidated data to the processed data directory
        df.to_csv(output_path, index=False, encoding='utf-8')

        print(f"\nConsolidation complete.")
        print(f"Master file saved: {output_path}")
        print(f"Total processed records: {len(df)}")
        print("\nDataset Preview (Cleaned Categories):")
        print(df.head(10).to_string(index=False))
    else:
        print("Data extraction failed: No valid records identified.")


if __name__ == "__main__":
    process_reports_to_master_csv()
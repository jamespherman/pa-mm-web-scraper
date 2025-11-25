# infographic_generator.py
import pandas as pd
import os
import datetime
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch

def generate_pdf_report(dataframe):
    """
    Generates a multi-page PDF report of top value products by store.
    """
    print("--- Generating PDF Best Value Report ---")

    # --- 1. Prepare Data ---
    df = dataframe.copy()
    df_filtered = df[(df['dpg'].notna()) & (df['dpg'] > 0) & (df['Total_Terps'].notna()) & (df['Total_Terps'] > 0)].copy()
    df_filtered['Value_Score'] = df_filtered['Total_Terps'] / df_filtered['dpg']

    # --- 2. Setup PDF ---
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    save_dir = os.path.join('figures', today_str)
    os.makedirs(save_dir, exist_ok=True)
    filename = os.path.join(save_dir, 'Best_Value_Report.pdf')

    doc = SimpleDocTemplate(filename)
    styles = getSampleStyleSheet()
    story = []

    # --- 3. Generate Content ---
    stores = df_filtered['Store'].unique()
    store_count = 0

    for store in stores:
        store_df = df_filtered[df_filtered['Store'] == store]

        # --- Store Header ---
        story.append(Paragraph(f"<b>{store}</b>", styles['h2']))
        story.append(Spacer(1, 0.2*inch))

        for category in ['Flower', 'Vaporizers', 'Concentrates']:
            category_df = store_df[store_df['Type'] == category]

            if not category_df.empty:
                top_5 = category_df.nlargest(5, 'Value_Score')

                # --- Category Header ---
                story.append(Paragraph(f"<u>{category}</u>", styles['h3']))
                story.append(Spacer(1, 0.1*inch))

                for _, row in top_5.iterrows():
                    p_text = f"<b>{row['Name_Clean']}</b> ({row['Brand']}) - ${row['Price']:.2f} | {row['Total_Terps']:.2f}% Terps | Score: {row['Value_Score']:.2f}"
                    story.append(Paragraph(p_text, styles['Normal']))
                story.append(Spacer(1, 0.2*inch))

        store_count += 1
        if store_count % 5 == 0 and store_count != len(stores):
            story.append(PageBreak())

    # --- 4. Build PDF ---
    if not story:
        print("No data to generate PDF report.")
        return

    try:
        doc.build(story)
        print(f"SUCCESS: Saved PDF report to {filename}")
    except Exception as e:
        print(f"ERROR: Failed to generate PDF report. Reason: {e}")

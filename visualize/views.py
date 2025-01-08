import io
import json
import pickle
import base64

import pandas as pd
import matplotlib.pyplot as plt
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse

plt.switch_backend('agg')
col_len = 22

def filter_columns(df, selected_drugs, common_columns):
    categories = set([col.split("_")[1] for col in df.columns[col_len:]]) - {"Claims", "Patients"}
    drug_columns = []
    for drug in selected_drugs:
        drug_columns.extend([f"{drug}_{category}" for category in categories])
        drug_columns.extend([f"{drug}_Patients", f"{drug}_Claims"])
    available_columns = [col for col in common_columns + drug_columns if col in df.columns]
    
    # Filter the DataFrame to include only the selected columns
    filtered_df = df[available_columns]
    
    # Convert the NPI column to string type
    if 'NPI' in filtered_df.columns:
        filtered_df.loc[:, 'NPI'] = filtered_df['NPI'].astype(str)
    
    # Process the 'Provider Business Mailing Address Postal Code' column
    if 'Provider Business Mailing Address Postal Code' in filtered_df.columns:
        filtered_df['Provider Business Mailing Address Postal Code'] = (
            filtered_df['Provider Business Mailing Address Postal Code']
            .astype(str)
            .str.zfill(5)
            .apply(lambda x: x.zfill(9) if len(x) > 5 else x)
            .apply(lambda x: f"{x[:5]}-{x[5:]}" if len(x) == 9 else x)
        )
    
    return filtered_df


def sum_and_sort_columns(df, keywords):
    # Keywords to filter columns
    # keywords = ['CONSULTING', 'EDUCATION', 'FOOD&BEVERAGE', 'GENERAL', 'SPEAKER', 'TRAVEL', "OTHERS", "OTHERS_GENERAL",'Claims','Patients']
    
    # Calculate the total sum of columns containing the keywords
    df.loc[:, 'Total Sum'] = df.filter(regex='|'.join(keywords)).sum(axis=1)
    
    # Filter out rows where the total sum is 0
    df = df[df['Total Sum'] > 0]

    # df.loc[:, 'Zip code'] = df['Zip code'].astype(str).apply(lambda x: x.zfill(9) if len(x) in [6, 7, 8] else x.zfill(5) if len(x) == 5 else x)

    
    # Sort the DataFrame by 'Total Sum' in descending order
    df = df.sort_values(by='Total Sum', ascending=False)
    
    # Drop the 'Total Sum' column before displaying
    df = df.drop(columns=['Total Sum'])
    
    return df


def generate_visualizations(df, selected_drugs):
    patients_totals = pd.Series(dtype=float)
    claims_totals = pd.Series(dtype=float)
    inner_graph = []

    for drug in selected_drugs:
        # columns = {
        #     'CONSULTING': f"{drug}_CONSULTING",
        #     'EDUCATION': f"{drug}_EDUCATION",
        #     'FOOD&BEVERAGE': f"{drug}_FOOD&BEVERAGE",
        #     'GENERAL': f"{drug}_GENERAL",
        #     'SPEAKER': f"{drug}_SPEAKER",
        #     'TRAVEL': f"{drug}_TRAVEL",
        #     "OTHERS":f"{drug}_OTHERS",
        #     "OTHERS_GENERAL":f"{drug}_OTHERS_GENERAL"

        # }
        columns = {}
        for col in df.columns[col_len:]:
            columns[col.split("_")[1]] = f"{drug}_{col.split('_')[0]}"

        available_columns = {label: col for label, col in columns.items() if col in df.columns}
        
        if available_columns:
            totals = df[list(available_columns.values())].sum()
            
            fig_pie, ax_pie = plt.subplots(figsize=(16, 9))
            wedges, texts = ax_pie.pie(
                totals, labels=None, autopct=None, startangle=140,
                pctdistance=0.85, shadow=True, wedgeprops=dict(width=0.3)
            )
            ax_pie.set_title(f"Distribution of Payments for {drug}")
            ax_pie.legend(wedges, totals.index, title="Payment Types", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            plt.close(fig_pie)
            buf.seek(0)
            # img = Image.open(buf)
            inner_graph.append(buf.getvalue())
            buf.close()

            fig_bar, ax_bar = plt.subplots(figsize=(16, 9))
            ax_bar.bar(totals.index, totals.values, color='skyblue')
            ax_bar.set_xlabel('Payment Type')
            ax_bar.set_ylabel('Total Amount')
            ax_bar.set_title(f"Total Payments for {drug}")
            ax_bar.tick_params(axis='x', rotation=45)            
            plt.close(fig_bar)
            

    available_columns = {
        'CLAIMS': f"{drug}_Claims",
        'PATIENTS': f"{drug}_Patients"
    }
    for drug in selected_drugs:
        if available_columns:
            if f"{drug}_Patients" in df.columns:
                patients_totals[drug] = df[f"{drug}_Patients"].fillna(0).sum()
            if f"{drug}_Claims" in df.columns:
                claims_totals[drug] = df[f"{drug}_Claims"].fillna(0).sum()

    def plot_bar_chart(data, title, ylabel):
        if data.empty or data.sum() == 0:
            return

        fig_bar, ax_bar = plt.subplots(figsize=(12, 8))
        data.plot(kind='bar', ax=ax_bar, color='skyblue')
        ax_bar.set_title(title)
        ax_bar.set_ylabel(ylabel)
        ax_bar.set_xlabel("Drugs")
        plt.xticks(rotation=45, ha='right')
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        plt.close(fig_bar)
        buf.seek(0)
        # img = Image.open(buf)
        inner_graph.append(buf.getvalue())
        buf.close()

    if not patients_totals.empty:
        plot_bar_chart(patients_totals, "Total Patients by Drug", "Number of Patients")

    if not claims_totals.empty:
        plot_bar_chart(claims_totals, "Total Claims by Drug", "Number of Claims(in Millions)")

    return inner_graph


def index(request):
    file_path = request.GET.get("file_path", "")
    df = pd.read_csv(file_path[1:])
    drug_cols = df.columns[col_len:]
    drugs = set([col.split("_")[0] for col in drug_cols])
    return render(request, "visualize.html", {"drugs": drugs, "file_path": file_path})


def filter(request):
    data = json.loads(request.body)
    file_path = data.get("file_path", "")
    selected_drugs = data.get("selected_drugs", [])
    
    df = pd.read_csv(file_path[1:])
    drug_cols = df.columns[col_len:]
    keywords = set([col.split("_")[1] for col in drug_cols])
    all_drugs = set([col.split("_")[0] for col in drug_cols])
    common_columns = list(df.columns[:col_len])

    all_drugs_option = "All"
    if all_drugs_option in selected_drugs:
        selected_drugs = all_drugs

    split = 22
    
    if selected_drugs:
        filtered_df = filter_columns(df, selected_drugs, common_columns)
        filtered_df = sum_and_sort_columns(filtered_df, keywords)
        filtered_df = filtered_df.reset_index(drop=True)
        filtered_df = filtered_df.fillna(0)

    graphs = generate_visualizations(filtered_df, selected_drugs)
    filtered_df["Provider Business Mailing Address Postal Code"] = filtered_df["Provider Business Mailing Address Postal Code"].apply(lambda x: x[:-2])
    filtered_df.loc[:, "Primary_Classification":"Org_pac_id_4"] = filtered_df.loc[:, "Primary_Classification":"Org_pac_id_4"].replace(0, '')
    resp = filtered_df.to_dict(orient='records')
    with open(f"plots/{request.user.username}.pkl", "wb") as f:
        pickle.dump(graphs, f)
    return JsonResponse(resp, safe=False)


def graphs(request):
    with open(f"plots/{request.user.username}.pkl", "rb") as f:
        plots = pickle.load(f)
    return JsonResponse([base64.b64encode(img).decode("utf-8") for img in plots], safe=False)
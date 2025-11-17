import pandas as pd
import os
from fpdf import FPDF
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='pkg_resources')


class PDF(FPDF):
    def header(self):
        self.set_font("Arial", "B", 12)
        self.cell(0, 10, "KO Reconciliation Report", ln=True, align="C")
        self.ln(5)

    def chapter_title(self, title):
        self.set_font("Arial", "B", 11)
        self.cell(0, 8, title, ln=True)
        self.ln(2)

    def add_table(self, df):
        if df is None or df.empty:
            self.set_font("Arial", "I", 9)
            self.cell(0, 8, "No data available", ln=True)
            self.ln(2)
            return

        epw = self.w - 2 * self.l_margin
        col_width = epw / len(df.columns)
        self.set_font("Arial", "B", 9)
        for col in df.columns:
            self.cell(col_width, 7, str(col)[:20], border=1)
        self.ln()

        self.set_font("Arial", "", 8)
        for _, row in df.iterrows():
            for val in row:
                self.cell(col_width, 6, str(val)[:20], border=1)
            self.ln()
        self.ln(3)


def run_reconciliation(admin_file, echeque_file, yono_file, selected_date, output_dir="."):
    """
    Run reconciliation logic (from original recon.py) and return key DataFrames + report file paths.
    """

    # --- Load Admin file ---
    admin = pd.read_excel(admin_file, sheet_name='BCKOLimitsConfiguration', skiprows=3)
    admin = admin[['Date of Transaction', 'Limit Configured By', 'KO ID', 'Opening Limit',
                   'Type of Transaction', 'Amount', 'Closing Limit']]
    admin.columns = admin.columns.str.strip().str.lower()

    # --- Load ECHEQUE file ---
    df = pd.read_excel(echeque_file, header=None)
    df = df.iloc[:, :7]
    df.columns = [
        "sl no/date of transaction",
        "limit configured by",
        "ko id",
        "opening limit",
        "transaction type",
        "amount",
        "closing limit"
    ]
    echeque = df.copy()
    echeque.columns = echeque.columns.str.strip().str.lower()

    # --- Load YONO file ---
    df = pd.read_csv(yono_file, header=None, low_memory=False)
    headers = df.iloc[19].tolist()
    yono = df.iloc[20:].copy()
    yono.columns = [str(col).strip().lower() for col in headers]

    # --- Transaction analysis functions ---
    def analyze_transactions(df, name):
        transaction_col = 'type of transaction' if 'type of transaction' in df.columns else 'transaction type'
        amount_col = 'amount'
        df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce')
        withdrawals = df[df[transaction_col] == 'KO Withdrawal']
        deposits = df[df[transaction_col] == 'KO Deposit']
        return {
            'num_withdrawals': len(withdrawals),
            'num_deposits': len(deposits),
            'sum_withdrawals': withdrawals[amount_col].sum(),
            'sum_deposits': deposits[amount_col].sum()
        }

    admin_counts = analyze_transactions(admin, "admin")
    echeque_counts = analyze_transactions(echeque, "echeque")

    def analyze_yono_combined(df):
        csp = df[df['description'].str.contains('cspcashsend', case=False, na=False)].copy()
        csp['debit'] = pd.to_numeric(csp['debit'], errors='coerce')
        csp = csp[csp['debit'] > 0]
        sum_csp_debit = csp['debit'].sum()
        count_csp_debit = len(csp)

        no_at = df[~df['description'].str.contains('@', na=False)].copy()
        no_at['credit'] = pd.to_numeric(no_at['credit'], errors='coerce')
        no_at = no_at[no_at['credit'] > 0]
        sum_no_at_credit = no_at['credit'].sum()
        count_no_at_credit = len(no_at)

        return {
            'count_csp_debit': count_csp_debit,
            'sum_csp_debit': sum_csp_debit,
            'count_no_at_credit': count_no_at_credit,
            'sum_no_at_credit': sum_no_at_credit
        }

    def analyze_yono_branch_99922(df):
        df['branch code'] = df['branch code'].astype(str).str.strip().str.split('.').str[0]
        branch_99922 = df[df['branch code'] == '99922'].copy()
        branch_99922['debit'] = pd.to_numeric(branch_99922['debit'], errors='coerce')
        branch_99922['credit'] = pd.to_numeric(branch_99922['credit'], errors='coerce')
        return {
            'debit_count': branch_99922['debit'].gt(0).sum(),
            'debit_sum': branch_99922['debit'].sum(),
            'credit_count': branch_99922['credit'].gt(0).sum(),
            'credit_sum': branch_99922['credit'].sum()
        }

    yono_counts = analyze_yono_combined(yono)
    _99922_counts = analyze_yono_branch_99922(yono)

    # --- Summary tables ---
    Withdrawal = pd.DataFrame({
        'No of Withdrawals': [admin_counts['num_withdrawals'], echeque_counts['num_withdrawals'],
                              yono_counts['count_csp_debit'], _99922_counts['debit_count']],
        'Sum of Withdrawals': [admin_counts['sum_withdrawals'], echeque_counts['sum_withdrawals'],
                               yono_counts['sum_csp_debit'], _99922_counts['debit_sum']]
    }, index=['Admin', 'Cheque', 'YONO', '99922'])

    Deposit = pd.DataFrame({
        'No of Deposits': [admin_counts['num_deposits'], echeque_counts['num_deposits'],
                           yono_counts['count_no_at_credit'], _99922_counts['credit_count']],
        'Sum of Deposits': [admin_counts['sum_deposits'], echeque_counts['sum_deposits'],
                            yono_counts['sum_no_at_credit'], _99922_counts['credit_sum']]
    }, index=['Admin', 'Cheque', 'YONO', '99922'])

    Total = pd.DataFrame({
        'Total Transactions': Withdrawal['No of Withdrawals'] + Deposit['No of Deposits'],
        'Total Sum': Withdrawal['Sum of Withdrawals'] + Deposit['Sum of Deposits']
    })

    # --- Dummy unmatched (for compatibility) ---
    total_unmatched_df = pd.DataFrame({'Sample': ['No unmatched data available']})
    withdrawal1_unmatched_df = pd.DataFrame({'Sample': ['No unmatched data available']})
    combined_unmatched_admin_yono = pd.DataFrame({'Sample': ['No unmatched data available']})
    combined_unmatched_admin_99922 = pd.DataFrame({'Sample': ['No unmatched data available']})
    deposit_unmatched_df = pd.DataFrame({'Sample': ['No unmatched data available']})

    # --- Save Excel ---
    excel_path = os.path.join(output_dir, f"reconciliation_report_{selected_date}.xlsx")
    with pd.ExcelWriter(excel_path) as writer:
        Withdrawal.to_excel(writer, sheet_name='Withdrawal')
        Deposit.to_excel(writer, sheet_name='Deposit')
        Total.to_excel(writer, sheet_name='Total')

    # --- Save PDF ---
    pdf_path = os.path.join(output_dir, f"reconciliation_report_{selected_date}.pdf")
    pdf = PDF()
    pdf.add_page()
    pdf.chapter_title("1. Withdrawal Summary")
    pdf.add_table(Withdrawal.reset_index())
    pdf.chapter_title("2. Deposit Summary")
    pdf.add_table(Deposit.reset_index())
    pdf.chapter_title("3. Total Summary")
    pdf.add_table(Total.reset_index())
    pdf.output(pdf_path)

    # Return results for Flask display
    return {
        "Withdrawal": Withdrawal,
        "Deposit": Deposit,
        "Total": Total,
        "Total_Unmatched": total_unmatched_df,
        "Withdrawal_Unmatched_Admin_Echeque": withdrawal1_unmatched_df,
        "Withdrawal_Unmatched_Admin_Yono": combined_unmatched_admin_yono,
        "Withdrawal_Unmatched_Admin_99922": combined_unmatched_admin_99922,
        "Deposit_Unmatched_Admin_Yono": deposit_unmatched_df,
        "Excel_File": excel_path,
        "PDF_File": pdf_path
    }

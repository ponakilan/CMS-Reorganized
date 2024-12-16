import sys

from control import initiate_processing


def main(public_dir, private_workbook, output):
    public_files = {
        "cms_b": "Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2022.csv",
        "cms_d": "MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv",
        "openpay": "OP_DTL_GNRL_PGYR2023_P06282024_06122024.csv",
        "nppes": "npidata_pfile_20050523-20241110.csv",
        "nucc": "nucc_taxonomy_241.csv",
        "dac": "DAC_NationalDownloadableFile.csv",
        "phase-1": "phase_1.csv"
    }

    private_sheets = {
        "cms_b": 0,
        "cms_d": 1,
        "openpay": 2,
        "taxonomy": 3,
        "openpay-map": 4
    }

    initiate_processing(
        public_dir=public_dir,
        private_workbook=private_workbook,
        public_files=public_files,
        private_sheets=private_sheets,
        output=output
    )


if __name__ == '__main__':
    public_dir = sys.argv[1]
    private_workbook = sys.argv[2]
    output = sys.argv[3]
    main(public_dir, private_workbook, output)

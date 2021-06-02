# Core
import pandas as pd


class SQMReader:
    def __init__(self, project_path):
        """
        """
        if not project_path.exists():
            raise Exception(f'{project_path} does not exist')

        self._project_path = project_path
        self._dir_name = project_path.name
        self.clean_data = self.load_clean_data()

    @property
    def project_path(self):
        return self._project_path

    @property
    def dir_name(self):
        return self._dir_name

    def load_clean_data(self):
        clean_file = self.project_path/f'{self.dir_name}_clean.csv'
        if not clean_file.exists():
            self.generate_clean_data()
        clean_data = pd.read_csv(clean_file,
                                 parse_dates=['dt_utc', 'dt_local'])

        return clean_data

    def generate_clean_data(self):
        data = self.join_dat_files(self.project_path/'raw', add_file_name=True)
        data = self.remove_duplicates(data).sort_values(by='dt_local')
        clean_file = f'{self.dir_name}_clean.csv'
        data.to_csv(self.project_path/clean_file, index=False)

    @staticmethod
    def read_sqm_file(file_path):
        with file_path.open(mode='r') as f:
            check = f.readlines()[37]
            if check != '# END OF HEADER\n':
                raise Exception('Not a SQM file')
        df = pd.read_csv(
            file_path, skiprows=38, header=None, sep=';',
            names=[
                'dt_utc', 'dt_local', 'temperature', 'voltage', 'msas',
                'record_type'],
            parse_dates=['dt_utc', 'dt_local']
        )

        return df

    @staticmethod
    def join_dat_files(folder_path, *, add_file_name=False):
        dataframes = []
        for file_path in folder_path.glob('*.dat'):
            df = SQMReader.read_sqm_file(file_path)
            if add_file_name:
                df['file_name'] =  file_path.name
            dataframes.append(df)

        return pd.concat(dataframes)

    @staticmethod
    def remove_duplicates(df):
        df_copy = df.copy()
        df_copy.sort_values(by=['file_name', 'dt_local'], inplace=True)
        is_duplicated = df_copy.duplicated(subset=['dt_local'], keep='last')

        return df_copy[~is_duplicated]


class Night:
    def __init__(self, data, where, location):
        pass
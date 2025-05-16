try:
    df=pd.read_parquet(os.path.join(self.data_dir, f'{self.current_date.strftime("%Y-%m-%d")}.parquet'))
    print(df,"dwsadwad")
except:
    print("nodata")
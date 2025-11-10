# Data Folder

This folder contains **all datasets used in your project**.  
It should include both raw data (as downloaded) and any processed or cleaned versions used for analysis.
If you are not downloading a raw dataset but instead loading it dynamically (e.g. through TidyTuesday), you should still have at least one processed dataset that can be used for your analysis. 

## Structure

```text
data/
├── raw/       # Unmodified source data
├── processed/ # Cleaned, merged, or filtered data ready for modeling
```

## Expectations
- **`/raw`**  
  - Store all original datasets exactly as obtained (CSV, JSON, etc.).  
  - Do not manually edit these files.  
  - Include a short description or link to the data source in this README or as a separate `data_dictionary.md` in this folder.
 
- **`/processed`**  
  - Contains data files created by your scripts (e.g., cleaned datasets, merged tables).  
  - Files in this folder should be reproducible by running scripts from `/src`.  

If your data files are large (>100 MB) then include a small sample dataset here for reproducibility. 

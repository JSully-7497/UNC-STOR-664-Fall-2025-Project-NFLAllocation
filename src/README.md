# Source Code Folder (`/src`)

This folder contains all code used in the project.

## Example Structure
```text
src/
├── 01_load_data.R        # Data import and cleaning
├── 02_eda.R              # Explore the data, make new datasets as needed 
├── 03_fit_models.R       # Model fitting and statistical analysis
├── 04_generate_figures.R # Visualization and table generation
```

## Expectations
- Scripts should start with a number, e.g. 01, 02, indicating the order in which they should be run.
- Each script should be self-contained and reproducible.  
- Include comments!  
- Scripts should save outputs (tables, figures) to `/results` as needed.

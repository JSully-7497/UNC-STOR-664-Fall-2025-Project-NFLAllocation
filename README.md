# STOR 664 Project Folder Template

This template is designed to provide an overview of how the repository should be structured for this project.

The project will be completed in two stages:

**1) Introduction and Data Exploration (due Sunday 11/16/2025)**

**2) Analysis, Results, and Discussion (due Wednesday 12/3/2025)**

In **Part 1)**, you will be:
- Determining what question you want to answer
  - What is your null hypothesis(es)?
- Identifying your dataset
- Looking for existing literature/projects about this question
- Producing at least one figure from your data that can be used towards answering your question
- Pointing out and addressing any concerns about your data's ability to answer the question (data wrangling)
- Proposing an analysis plan for your data

In **Part 2)** you will be:
- Carrying out your full statistical analysis regarding your hypothesis
- Discussing your analytical decisions (why did you choose your models, how did you account for any limitations)
- Examining your results with figures and/or tables
- Answering the questions:
    - Were your answers conclusive? If not, what could have been done to make them conclusive?
    - Were your assumptions reasonable?
    - What limitations did your data and/or analysis have?
- Discussing future extensions for your work

## Workflow Structure:
As you go through your analysis, you will develop the stages of your project in feature branches within this repository. 

Each part of your project should have one branch created, on which all team members will contribute. Reports can be created using a tool of your choice (MS Word, Latex, Quarto etc) and source files do not need to be tracked in the repository, but the final report for each part **must** be uploaded to the `\reports` folder.

Prior to the submission deadline for the applicable project part, a pull request should be opened with Dr. Kessler and Shaleni set as reviewers.

After addressing any feedback, the PR should be closed and the branch should be merged into main. 

## Deliverables:

### Part 1)
- The report for Part 1 is due 11/16/2025 at 11:59 PM. The report should be uploaded to Gradescope at this time and the pull request (PR) from your branch to main should be opened by this time as well (will be part of the Gradescope submission). Remember that your report must also be in the `\reports` folder at the time of your PR.
    - This part of the project will be peer reviewed. Don't forget to add your peer reviewers, Dr. Kessler, and Shaleni as reviewers on your PR!
- On 11/17/2025 you will be giving a 5 minute presentation (aim for 3 slides) on your project question, data, exploratory analyses, and proposal for analysis. 
- You will be assigned a project to peer-review. By 11/18/2025 at 11:59 PM you should have made at least two thoughtful comments on the PR of the project you are reviewing. Links to these comments will need to be submitted to Gradescope. You are encouraged to clone the repository and see if the work that has been done so far is reproducible!

### Part 2)
- The report for Part 2, your final project report, is due 12/03/2025 at 11:59 PM. As in Part 1, The report should be uploaded to Gradescope at this time and the pull request (PR) from your branch to main should be opened by this time as well, with a copy of the report in the `\reports` folder. 

---

## What to Do

1) **Set up your repository**  
    - Create a new repository using this one as a template (look for the blue button at the top right). 
    - Make sure Dr. Kessler (@dankessler) and Shaleni Kovach (@Shalenik) are invited to the repository.
2) **Review existing documentation**
    - Each folder in this repository contains a README that outlines the use and expectations for that folder. It is recommended to review these *before* starting to avoid unnecessary rework.
4) **Document your work**  
    - Update your `README.md` following the directions below.
    - Keep all code, data, and reports within the appropriate folders (outlined below).
    - Contribution guidelines are also included below.
5) **Submit your reports**  
    - Upload your completed reports to **GradeScope**.  
    - Ensure the same reports are available in your `/report` folder on GitHub.

---

## README Setup
In your README.md there should be five sections:

1) Project Name
2) Team Members and their github ids
3) Overview or description of project
4) Overview or description of repository
5) Basic usage explanation

For example:


# stor-664-project

## Team Members
- Jack 
- Ani
- Jaehyuk
- Soumyjyoti

## Overview
This repository contains the group project for [STOR 664, Fall 2025].  
Our goal is to analyze nfl resource allocation using data from nflreadr and linear regression methods
Results show ...

## Repository Structure
| Folder | Purpose | Key Files |
|---------|----------|-----------|
| `/data/raw` | Original unmodified datasets | `data1.csv`, `data2.csv` |
| `/data/processed` | Cleaned datasets ready for analysis | `merged_data.rds` |
| `/src` | Analysis and visualization code | `02_fit_models.R`, `03_generate_figures.R` |
| `/results/tables` | Numeric summaries | `model_performance.csv` |
| `/results/figures` | Visual outputs | `figure1.png` |
| `/report` | All written deliverables | `01_introduction.md`, `final_report.qmd` |

## Getting Started
### 1. Clone the repository

```bash
git clone <your .git url>
cd stor-664-project-sample
```

### 2. Install dependencies (optional but highly recommended)
Example in R:
```r
renv::restore()
```

Example in Python:
The python files in src use uv. Make sure uv is installed on your machine. Then cd into the directory for the repo, and run uv sync. This will create a virtual environment with all the dependences. From there type,
source .venv/bin/activate and your environment will be activated with all dependencies installed. 

### 3. Running Analysis Scripts
For the python scripts you can run the script using uv run 'path to project'. Or use vs code and click the little green play button.

---

## Contributing

1) Create a feature branch for each component of the project. For example, for the methods deliverable:
```bash
git checkout -b methods
```

2) Every team member should be working on this branch for the methods component of the project.
   - Use concise, meaningful messages:
     ```
     git commit -m "Add OLS model fitting script"
     ```
4) Before the submission deadline, open a PR and set Dr. Kessler and Shaleni as reviewers. If this is a peer reviwed component, make sure your reviewer(s) have access to the repository and are also assigned as reviewers on the PR. A link to your PR will need to be included as part of your Gradescope submission.
5) Incorporate feedback as necessary, update the feature branch, and close the PR before moving to the next component.
   - Your next feature branch should be created from your newly updated `main` branch.
   - `main` should only be updated from one of the feature branches after a PR.

## Peer Reviewing

For one component of the project you will be asked to peer-review another team's pull request. When doing so, make sure to leave **at least two** constructive comments. You will be asked to provide links to your two comments in Gradescope.

Your peer reviewers should be able to clone and run your repository to reproduce your results.

## Reports

Any medium can be used to generate the reports (MS Word, Latex, Quarto etc), however the final result will need to be a PDF file that will be placed in the `\reports` folder as well as uploaded on Gradescope.

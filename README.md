Overview
========

Welcome to the Customer Success Astro Onboarding Demos! This project was created to highlight various integrations with Airflow that are commonly brought up by customers during onboarding. This project contains several working DAGs that have been deployed to [this environment](https://cloud.astronomer.io/cl1vvhd0c85301fyobcagec1c/deployments/cl3sutkeb118541hynkfcnlnmk)

Contributing
===========================

A multi-branch CICD process with branch protection rules has been incorporated with this project (for demonstration purposes). In order to contribute to this project, please follow these steps:

1. Checkout the `dev` branch using `git checkout dev`
2. Run a `git pull` command to ensure you have the most recent changes
3. Checkout a feature branch using `git checkout -b <feature-name>`
4. Develop your changes locally
5. Run a `git push --set-upstream origin <feature-name>` to push your local changes
6. Navigate to [this repository](https://github.com/astronomer/cs-astro-onboarding/) on GitHub via a web browser and open a PR from your feature branch to the `dev` branch
7. Request a review from a CSE (this isn't enforced by branch protection rules - in the event you are demo'ing this process for a customer)
8. Upon approval of the review, merge your feature branch to `dev` this will initiate a GitHub Action that performs the following:

    - Deploy changes to [this Dev Environment](https://cloud.astronomer.io/cl1vvhd0c85301fyobcagec1c/deployments/cl530xesf295741i1cg7jpjgnt)
    - Opens a new PR from `dev` to `master`

9. Review the new PR to `master` by ensuring the changes didn't break the Dev Deployment
10. Upon successful inspection of the Dev Deployment, merge the PR into `master` this will initiate a GitHub Action that performs the following:

    - Deploy changes to [this Prod Environment](https://cloud.astronomer.io/cl1vvhd0c85301fyobcagec1c/deployments/cl3sutkeb118541hynkfcnlnmk)

![cicd_demo](https://user-images.githubusercontent.com/31361051/177388711-6e9bc598-b5eb-4fd4-b445-cacf0e4d2fd2.png)

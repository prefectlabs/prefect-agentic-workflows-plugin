# Deeper interview

Offer this after the checklist when the workflow has more than one branch, more than one approval, or touches more than two systems. It finds the assumptions the checklist leaves open. Ask one group at a time, and skip any question the user has already answered.

## For each step

- What does this step decide, and what are the possible outcomes?
- What does it read, and from where: a run input, an earlier step, or an outside system?
- What does it hand to the next step? Name the fields.
- Which tools and credentials does it use? Did the infrastructure check find each tool reachable?
- Could it take longer than 60 seconds? If so, where can it be split?
- Is it the same every time, like a script or a data load? If so, which deployment from the infrastructure check runs it?

## For each approval

- Who approves, and what do they need to see to decide?
- What are the choices? Each choice becomes an output of the approval node.
- What happens when nobody answers? Is there a deadline, and which choice applies when it passes?

## Failures

- When a step fails, should the run stop, retry the step, or continue on another path?
- Which failures should reach a person, and how?

## Runs and results

- Which values change from run to run? These become plan inputs.
- Who reads the result, and in what form?
- Will the workflow run on a schedule? How often, and in which time zone?

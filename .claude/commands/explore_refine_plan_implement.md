# Explore
First, use parallel subagents to find and read all files that may be useful for implementing the ticket, start with the markdown files in the .agent folder. Use what is applicable from there to continue with the ask. The subagents should return relevant file paths, and any other info that may be useful. If the inital request needs refinement and more contrete examples you ask for it.

# Plan
Next, think hard and write a detailed implementation plan. Include tests and verifiable steps to verify sound progress during implementation. Use your judgment as to what is necessary, given the standard and best practices you read in the previous step. 

If there are things you still do not understand or questions you have for the user pause here and and ask user to clarify.

When you are happy with the plan. Create a new md file in .agent_plans with a meaningful name for the ask. Create a map over the plan and use todo lists for the individual steps.  

# Code 
When you have a thorough implementation plan, you are ready to start writing code. For each implementing step, you update the md file in agent_plans todo lists. Follow the style of the existing codebase (e.g. we prefer clearly named variables and methods to extensive comments). Make sure to run our autoformatting script when you're done, and fix linter warnings that seem reasonable to you

# Test
Use parallel subagents to run tests, and make sure they all pass.

If your changes touch the UX in a major way, use the browser to make sure that everything works correctly. Make a list of what to test for, and use a subagent for this step.

If your testing shows problems, go back to the planning stage and think ultrahard.

# Write up you work
Write up your work
When you are happy with your work, write up a short report that could be used as the PR description. Include what you set out to do, the choices you made with their brief justification, and any commands you ran in the process that may be useful for future developers to know about.



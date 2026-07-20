# Review

Review the completed work on this branch for correctness and regressions. Read
the diff against the default branch and run the tests that cover it. Do not
change the implementation.

Then report the verdict, exactly once, as your final step:

    sloop verdict pass --reason "<concise reason>"
    sloop verdict fail --reason "<concise reason>"

That command — not your prose — is what decides this stage. Finishing without
it settles the stage as a failure with `no verdict reported`.

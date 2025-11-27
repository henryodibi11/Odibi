from typing import Any

from odibi.context import EngineContext
from odibi.patterns.base import Pattern
from odibi.transformers.merge_transformer import MergeParams, merge


class MergePattern(Pattern):
    """
    Merge Pattern: Upsert/Merge logic.

    Params:
        target (str): Target table/path.
        keys (list): Join keys.
        strategy (str): 'upsert', 'append_only', 'delete_match'.
    """

    def validate(self) -> None:
        if not self.params.get("target"):
            raise ValueError("MergePattern: 'target' is required.")
        if not self.params.get("keys"):
            raise ValueError("MergePattern: 'keys' is required.")

    def execute(self, context: EngineContext) -> Any:
        # Map dictionary params to MergeParams model
        valid_keys = MergeParams.model_fields.keys()
        filtered_params = {k: v for k, v in self.params.items() if k in valid_keys}

        # Execute logic
        # The merge transformer performs the write/merge operation and returns the source_df
        # (or Query object for streaming).
        # Since the pattern execution happens BEFORE the Node's write phase,
        # we need to be careful.
        # If 'merge' transformer writes to the target, then the Node's write phase should probably be skipped
        # or point to a dummy location?
        # OR the MergePattern should just prepare the data?

        # In odibi/transformers/merge_transformer.py, the `merge` function performs the ACTUAL write/merge.
        # It returns the source_df.

        # Issue: If Pattern executes the Write, and Node also executes Write, we write twice.
        # But Node config might not have a 'write' block if the pattern handles it?
        # The Node validation requires "at least one of read, transform, write, transformer".
        # If 'transformer' is set, 'write' is optional in NodeConfig?
        # Let's check NodeConfig model.
        # Yes, 'at_least_one_operation' check.

        # So if user configures:
        # - name: my_merge
        #   transformer: merge
        #   params: ...
        #   # No write block

        # Then Node._execute_write_phase will do nothing (if self.config.write is None).
        # This is correct. The Pattern handles the persistence.

        merge(context, context.df, **filtered_params)

        # Return the dataframe for potential downstream usage (though likely end of chain)
        return context.df

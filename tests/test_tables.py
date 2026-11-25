from rts.config import DeltaMergeWrite
from rts.tables import merge_sql


def test_merge_updates_every_matched_row_without_filters() -> None:
    merge = DeltaMergeWrite(
        table='sales.orders',
        on=['old.order_id = new.order_id', 'old.ordered_on = new.ordered_on'],
        columns={'amount': 'new.amount', 'status': 'coalesce(new.status, old.status)'},
    )

    assert merge_sql(merge) == (
        'merge into sales.orders as old using source as new '
        'on old.order_id = new.order_id and old.ordered_on = new.ordered_on '
        'when matched '
        'then update set old.amount = new.amount, '
        'old.status = coalesce(new.status, old.status)'
    )


def test_merge_updates_only_rows_passing_a_filter() -> None:
    merge = DeltaMergeWrite(
        table='sales.orders',
        on=['old.order_id = new.order_id'],
        columns={'ordered_at': 'new.ordered_at'},
        filters={'ordered_at': '>', 'amount': '!='},
    )

    assert merge_sql(merge) == (
        'merge into sales.orders as old using source as new '
        'on old.order_id = new.order_id '
        'when matched and ('
        '(old.ordered_at is null and new.ordered_at is not null)'
        ' or old.ordered_at > new.ordered_at'
        ' or (old.amount is null and new.amount is not null)'
        ' or old.amount != new.amount'
        ') '
        'then update set old.ordered_at = new.ordered_at'
    )

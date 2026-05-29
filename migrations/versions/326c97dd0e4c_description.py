"""description

Revision ID: 326c97dd0e4c
Revises: d951021d009c
Create Date: 2026-05-29 11:36:41.139403

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '326c97dd0e4c'
down_revision: Union[str, Sequence[str], None] = 'd951021d009c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass

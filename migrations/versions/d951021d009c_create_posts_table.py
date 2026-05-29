"""create posts table

Revision ID: d951021d009c
Revises: 
Create Date: 2026-05-29 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd951021d009c'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'posts',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('content', sa.Text(), nullable=False),
        sa.Column('author', sa.String(length=50), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.CheckConstraint('char_length(content) <= 280', name='content_length_check')
    )


def downgrade() -> None:
    op.drop_table('posts')

"""empty message

Revision ID: 859086d5263f
Revises: c9964df4e8bc
Create Date: 2022-07-27 10:49:57.899243

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '859086d5263f'
down_revision = 'c9964df4e8bc'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('users', sa.Column('phone', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('users', 'phone')
    # ### end Alembic commands ###
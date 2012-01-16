# Copyright 2012 OpenStack LLC.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from sqlalchemy import *

meta = MetaData()

zones = Table('zones', meta,
        Column('id', Integer(), primary_key=True, nullable=False),
        )

is_parent = Column('is_parent', Boolean(), default=False)
amqp_host = Column('amqp_host', String(255))
amqp_port = Column('amqp_port', Integer(), default=5672)
amqp_virtual_host = Column('amqp_virtual_host', String(255))


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    zones.create_column(is_parent)
    zones.create_column(amqp_host)
    zones.create_column(amqp_port)
    zones.create_column(amqp_virtual_host)


def downgrade(migrate_engine):
    meta.bind = migrate_engine

    zones.drop_column(amqp_virtual_host)
    zones.drop_column(amqp_port)
    zones.drop_column(amqp_host)
    zones.drop_column(is_parent)

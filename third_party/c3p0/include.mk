# Copyright (C) 2011-2012  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 2.1 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

C3P0_VERSION := 0.9.2
C3P0 := third_party/c3p0/c3p0-$(C3P0_VERSION).jar
C3P0_BASE_URL := http://repo1.maven.org/maven2/com/mchange/c3p0/$(C3P0_VERSION)

$(C3P0): $(C3P0).md5
	set dummy "$(C3P0_BASE_URL)" "$(C3P0)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(C3P0)

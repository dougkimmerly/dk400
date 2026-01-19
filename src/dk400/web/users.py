"""
DK/400 User Management

AS/400-style user profiles with password authentication.
Uses PBKDF2 for secure password hashing, PostgreSQL for storage.
"""
import hashlib
import secrets
import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

from src.dk400.web.database import (
    get_cursor, init_database, check_connection,
    create_role, drop_role, update_role_password, set_role_enabled,
    set_group_profile, copy_authorities_from, get_user_group
)

logger = logging.getLogger(__name__)


@dataclass
class UserProfile:
    """AS/400-style user profile - full DSPUSRPRF compatible."""
    # Identity
    username: str
    password_hash: str
    salt: str

    # Classification
    user_class: str = "*USER"       # *SECOFR, *SECADM, *PGMR, *SYSOPR, *USER
    status: str = "*ENABLED"        # *ENABLED, *DISABLED
    description: str = ""           # Text 'description'

    # Password
    password_expires: str = "*NOMAX"
    password_last_changed: str = ""
    password_expired: str = "*NO"
    signon_attempts: int = 0

    # Authority
    spcaut: list = None             # Special authorities
    group_profile: str = "*NONE"
    supgrpprf: list = None          # Supplemental groups
    owner: str = "*USRPRF"
    grpaut: str = "*NONE"
    grpauttyp: str = "*PRIVATE"

    # Initial program/menu
    inlpgm: str = "*NONE"
    inlpgm_lib: str = ""
    inlmnu: str = "MAIN"
    inlmnu_lib: str = "*LIBL"
    lmtcpb: str = "*NO"

    # Library list
    current_library: str = "QGPL"
    inllibl: list = None            # Initial library list

    # Output
    outq: str = "*WRKSTN"
    outq_lib: str = ""
    prtdev: str = "*WRKSTN"

    # Message queue
    msgq: str = ""
    msgq_lib: str = "QUSRSYS"
    dlvry: str = "*NOTIFY"
    sev: int = 0

    # Job description
    jobd: str = "QDFTJOBD"
    jobd_lib: str = "QGPL"

    # Attention program
    atnpgm: str = "*SYSVAL"
    atnpgm_lib: str = ""

    # Locale
    srtseq: str = "*SYSVAL"
    srtseq_lib: str = ""
    langid: str = "*SYSVAL"
    cntryid: str = "*SYSVAL"
    ccsid: str = "*SYSVAL"

    # Environment
    spcenv: str = "*NONE"
    astlvl: str = "*SYSVAL"
    dspsgninf: str = "*NO"
    lmtdevssn: str = "*SYSVAL"
    kbdbuf: str = "*SYSVAL"

    # Storage
    maxstg: str = "*NOMAX"
    curstrg: int = 0

    # Other
    acgcde: str = ""
    homedir: str = ""
    usropt: list = None
    objaud: str = "*NONE"
    audlvl: list = None

    # Timestamps
    created: str = ""
    last_signon: str = ""

    def __post_init__(self):
        """Initialize list fields."""
        if self.spcaut is None:
            self.spcaut = []
        if self.supgrpprf is None:
            self.supgrpprf = []
        if self.inllibl is None:
            self.inllibl = ["QGPL", "QSYS"]
        if self.usropt is None:
            self.usropt = []
        if self.audlvl is None:
            self.audlvl = []
        if not self.msgq:
            self.msgq = self.username

    @classmethod
    def from_row(cls, row: dict) -> 'UserProfile':
        """Create UserProfile from database row."""
        return cls(
            username=row['username'],
            password_hash=row['password_hash'],
            salt=row['salt'],
            user_class=row.get('user_class', '*USER'),
            status=row.get('status', '*ENABLED'),
            description=row.get('description', ''),
            password_expires=row.get('password_expires', '*NOMAX'),
            password_last_changed=str(row['password_last_changed']) if row.get('password_last_changed') else '',
            password_expired=row.get('password_expired', '*NO'),
            signon_attempts=row.get('signon_attempts', 0),
            spcaut=row.get('spcaut') or [],
            group_profile=row.get('group_profile', '*NONE'),
            supgrpprf=row.get('supgrpprf') or [],
            owner=row.get('owner', '*USRPRF'),
            grpaut=row.get('grpaut', '*NONE'),
            grpauttyp=row.get('grpauttyp', '*PRIVATE'),
            inlpgm=row.get('inlpgm', '*NONE'),
            inlpgm_lib=row.get('inlpgm_lib', ''),
            inlmnu=row.get('inlmnu', 'MAIN'),
            inlmnu_lib=row.get('inlmnu_lib', '*LIBL'),
            lmtcpb=row.get('lmtcpb', '*NO'),
            current_library=row.get('current_library', 'QGPL'),
            inllibl=row.get('inllibl') or row.get('library_list') or ["QGPL", "QSYS"],
            outq=row.get('outq', '*WRKSTN'),
            outq_lib=row.get('outq_lib', ''),
            prtdev=row.get('prtdev', '*WRKSTN'),
            msgq=row.get('msgq') or row['username'],
            msgq_lib=row.get('msgq_lib', 'QUSRSYS'),
            dlvry=row.get('dlvry', '*NOTIFY'),
            sev=row.get('sev', 0),
            jobd=row.get('jobd', 'QDFTJOBD'),
            jobd_lib=row.get('jobd_lib', 'QGPL'),
            atnpgm=row.get('atnpgm', '*SYSVAL'),
            atnpgm_lib=row.get('atnpgm_lib', ''),
            srtseq=row.get('srtseq', '*SYSVAL'),
            srtseq_lib=row.get('srtseq_lib', ''),
            langid=row.get('langid', '*SYSVAL'),
            cntryid=row.get('cntryid', '*SYSVAL'),
            ccsid=row.get('ccsid', '*SYSVAL'),
            spcenv=row.get('spcenv', '*NONE'),
            astlvl=row.get('astlvl', '*SYSVAL'),
            dspsgninf=row.get('dspsgninf', '*NO'),
            lmtdevssn=row.get('lmtdevssn', '*SYSVAL'),
            kbdbuf=row.get('kbdbuf', '*SYSVAL'),
            maxstg=row.get('maxstg', '*NOMAX'),
            curstrg=row.get('curstrg', 0),
            acgcde=row.get('acgcde', ''),
            homedir=row.get('homedir', ''),
            usropt=row.get('usropt') or [],
            objaud=row.get('objaud', '*NONE'),
            audlvl=row.get('audlvl') or [],
            created=str(row['created']) if row.get('created') else '',
            last_signon=str(row['last_signon']) if row.get('last_signon') else '',
        )


class UserManager:
    """Manages user profiles and authentication."""

    # Password hashing parameters
    HASH_ITERATIONS = 100000
    HASH_ALGORITHM = 'sha256'

    # Security parameters
    MAX_SIGNON_ATTEMPTS = 5  # Lock account after this many failures

    def __init__(self):
        self._initialized = False
        self._init_database()

    def _init_database(self):
        """Initialize database and ensure default users exist."""
        try:
            if not check_connection():
                logger.warning("Database not available, will retry on next operation")
                return

            init_database()
            # Set initialized BEFORE creating default users to prevent recursion
            self._initialized = True
            self._ensure_default_users()
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")

    def _ensure_initialized(self):
        """Ensure database is initialized before operations."""
        if not self._initialized:
            self._init_database()

    def _ensure_default_users(self):
        """Ensure default system users exist."""
        defaults = [
            ('QSECOFR', 'QSECOFR', '*SECOFR', 'Security Officer'),
            ('QSYSOPR', 'QSYSOPR', '*SYSOPR', 'System Operator'),
            ('QUSER', 'QUSER', '*USER', 'Default User'),
        ]

        for username, password, user_class, description in defaults:
            if not self.get_user(username):
                self.create_user(
                    username=username,
                    password=password,
                    user_class=user_class,
                    description=description
                )

    def _hash_password(self, password: str, salt: str) -> str:
        """Hash a password using PBKDF2."""
        key = hashlib.pbkdf2_hmac(
            self.HASH_ALGORITHM,
            password.encode('utf-8'),
            salt.encode('utf-8'),
            self.HASH_ITERATIONS
        )
        return key.hex()

    def _generate_salt(self) -> str:
        """Generate a random salt."""
        return secrets.token_hex(32)

    def create_user(
        self,
        username: str,
        password: str,
        user_class: str = "*USER",
        description: str = "",
        group_profile: str = "*NONE",
        copy_from_user: str = ""
    ) -> tuple[bool, str]:
        """Create a new user profile.

        Args:
            username: User ID (max 10 characters)
            password: Initial password
            user_class: *SECOFR, *SECADM, *PGMR, *SYSOPR, or *USER
            description: Text description
            group_profile: User to inherit authorities from (*NONE for none)
            copy_from_user: Copy object authorities from this user (optional)
        """
        self._ensure_initialized()

        username = username.upper().strip()
        group_profile = group_profile.upper().strip() if group_profile else "*NONE"
        copy_from_user = copy_from_user.upper().strip() if copy_from_user else ""

        if not username:
            return False, "Username is required"

        if len(username) > 10:
            return False, "Username must be 10 characters or less"

        if self.get_user(username):
            return False, f"User {username} already exists"

        if not password:
            return False, "Password is required"

        # Validate group profile exists if specified
        if group_profile and group_profile != "*NONE":
            if not self.get_user(group_profile):
                return False, f"Group profile {group_profile} not found"

        # Validate copy_from_user exists if specified
        if copy_from_user:
            if not self.get_user(copy_from_user):
                return False, f"Copy from user {copy_from_user} not found"

        salt = self._generate_salt()
        password_hash = self._hash_password(password.upper(), salt)

        try:
            with get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO qsys.qausrprf (
                        username, password_hash, salt, user_class,
                        status, description, group_profile, created
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    username,
                    password_hash,
                    salt,
                    user_class,
                    '*ENABLED',
                    description,
                    group_profile,
                    datetime.now(),
                ))

            # Create corresponding PostgreSQL role
            role_success, role_msg = create_role(username, password.upper(), user_class)
            if not role_success:
                logger.warning(f"User {username} created but role creation failed: {role_msg}")

            # Set group profile (PostgreSQL role inheritance)
            if group_profile and group_profile != "*NONE":
                grp_success, grp_msg = set_group_profile(username, group_profile)
                if not grp_success:
                    logger.warning(f"User {username} created but group profile failed: {grp_msg}")

            # Copy authorities from another user
            if copy_from_user:
                copy_success, copy_msg = copy_authorities_from(copy_from_user, username)
                if not copy_success:
                    logger.warning(f"User {username} created but copy authorities failed: {copy_msg}")

            return True, f"User {username} created"
        except Exception as e:
            logger.error(f"Failed to create user {username}: {e}")
            return False, f"Failed to create user: {e}"

    def delete_user(self, username: str) -> tuple[bool, str]:
        """Delete a user profile."""
        self._ensure_initialized()

        username = username.upper().strip()

        if username in ('QSECOFR', 'QSYSOPR', 'QUSER'):
            return False, f"Cannot delete system user {username}"

        if not self.get_user(username):
            return False, f"User {username} not found"

        try:
            with get_cursor() as cursor:
                cursor.execute(
                    "DELETE FROM qsys.qausrprf WHERE username = %s",
                    (username,)
                )

            # Drop corresponding PostgreSQL role
            role_success, role_msg = drop_role(username)
            if not role_success:
                logger.warning(f"User {username} deleted but role removal failed: {role_msg}")

            return True, f"User {username} deleted"
        except Exception as e:
            logger.error(f"Failed to delete user {username}: {e}")
            return False, f"Failed to delete user: {e}"

    def change_password(self, username: str, new_password: str) -> tuple[bool, str]:
        """Change a user's password."""
        self._ensure_initialized()

        username = username.upper().strip()

        if not self.get_user(username):
            return False, f"User {username} not found"

        if not new_password:
            return False, "Password is required"

        salt = self._generate_salt()
        password_hash = self._hash_password(new_password.upper(), salt)

        try:
            with get_cursor() as cursor:
                cursor.execute("""
                    UPDATE qsys.qausrprf
                    SET password_hash = %s, salt = %s
                    WHERE username = %s
                """, (password_hash, salt, username))

            # Update PostgreSQL role password
            role_success, role_msg = update_role_password(username, new_password.upper())
            if not role_success:
                logger.warning(f"Password changed for {username} but role update failed: {role_msg}")

            return True, f"Password changed for {username}"
        except Exception as e:
            logger.error(f"Failed to change password for {username}: {e}")
            return False, f"Failed to change password: {e}"

    def change_group_profile(self, username: str, new_group: str) -> tuple[bool, str]:
        """Change a user's group profile."""
        self._ensure_initialized()

        username = username.upper().strip()
        new_group = new_group.upper().strip() if new_group else "*NONE"

        if not self.get_user(username):
            return False, f"User {username} not found"

        # Validate new group exists if not *NONE
        if new_group and new_group != "*NONE":
            if not self.get_user(new_group):
                return False, f"Group profile {new_group} not found"

        # Use database function to set group profile
        success, msg = set_group_profile(username, new_group)
        if success:
            return True, f"Group profile changed for {username}"
        return False, msg

    def update_user(
        self,
        username: str,
        user_class: str = None,
        description: str = None,
        group_profile: str = None,
    ) -> tuple[bool, str]:
        """Update a user profile (CHGUSRPRF equivalent).

        Only non-None parameters are updated.
        """
        self._ensure_initialized()

        username = username.upper().strip()

        if not self.get_user(username):
            return False, f"User {username} not found"

        # Build update query dynamically
        updates = []
        values = []

        if user_class is not None:
            user_class = user_class.upper().strip()
            if user_class not in ('*SECOFR', '*SECADM', '*PGMR', '*SYSOPR', '*USER'):
                return False, f"Invalid user class: {user_class}"
            updates.append("user_class = %s")
            values.append(user_class)

        if description is not None:
            updates.append("description = %s")
            values.append(description.strip())

        if group_profile is not None:
            group_profile = group_profile.upper().strip() if group_profile else "*NONE"
            if group_profile and group_profile != "*NONE":
                if not self.get_user(group_profile):
                    return False, f"Group profile {group_profile} not found"
            updates.append("group_profile = %s")
            values.append(group_profile)
            # Also update PostgreSQL role
            set_group_profile(username, group_profile)

        if not updates:
            return True, "No changes specified"

        values.append(username)

        try:
            with get_cursor() as cursor:
                cursor.execute(
                    f"UPDATE qsys.qausrprf SET {', '.join(updates)} WHERE username = %s",
                    values
                )
            return True, f"User {username} changed"
        except Exception as e:
            logger.error(f"Failed to update user {username}: {e}")
            return False, f"Failed to update user: {e}"

    def authenticate(self, username: str, password: str) -> tuple[bool, str]:
        """Authenticate a user."""
        self._ensure_initialized()

        username = username.upper().strip()
        password = password.upper() if password else ""

        if not username:
            return False, "Username is required"

        user = self.get_user(username)
        if not user:
            # Don't reveal whether user exists
            return False, "User ID or password not valid"

        if user.status == "*DISABLED":
            return False, f"User profile {username} is disabled"

        # Check if account is locked due to failed attempts
        if user.signon_attempts >= self.MAX_SIGNON_ATTEMPTS:
            return False, f"User profile {username} is locked (too many failed attempts)"

        # Verify password
        password_hash = self._hash_password(password, user.salt)

        if password_hash != user.password_hash:
            # Increment failed attempts
            new_attempts = user.signon_attempts + 1
            try:
                with get_cursor() as cursor:
                    cursor.execute("""
                        UPDATE qsys.qausrprf
                        SET signon_attempts = %s
                        WHERE username = %s
                    """, (new_attempts, username))
            except Exception:
                pass

            # Check if this attempt locks the account
            remaining = self.MAX_SIGNON_ATTEMPTS - new_attempts
            if remaining <= 0:
                return False, f"User profile {username} is now locked"
            elif remaining <= 2:
                return False, f"User ID or password not valid ({remaining} attempts remaining)"
            return False, "User ID or password not valid"

        # Successful authentication - update last signon, reset attempts
        try:
            with get_cursor() as cursor:
                cursor.execute("""
                    UPDATE qsys.qausrprf
                    SET signon_attempts = 0, last_signon = %s
                    WHERE username = %s
                """, (datetime.now(), username))
        except Exception:
            pass

        return True, "Sign on successful"

    def unlock_user(self, username: str) -> tuple[bool, str]:
        """Unlock a user account (reset signon attempts)."""
        self._ensure_initialized()

        username = username.upper().strip()

        if not self.get_user(username):
            return False, f"User {username} not found"

        try:
            with get_cursor() as cursor:
                cursor.execute("""
                    UPDATE qsys.qausrprf
                    SET signon_attempts = 0
                    WHERE username = %s
                """, (username,))
            return True, f"User {username} unlocked"
        except Exception as e:
            logger.error(f"Failed to unlock user {username}: {e}")
            return False, f"Failed to unlock user: {e}"

    def get_user(self, username: str) -> Optional[UserProfile]:
        """Get a user profile."""
        self._ensure_initialized()

        username = username.upper().strip()

        try:
            with get_cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM qsys.qausrprf WHERE username = %s",
                    (username,)
                )
                row = cursor.fetchone()
                if row:
                    return UserProfile.from_row(row)
        except Exception as e:
            logger.error(f"Failed to get user {username}: {e}")

        return None

    def list_users(self) -> list[UserProfile]:
        """List all user profiles."""
        self._ensure_initialized()

        users = []
        try:
            with get_cursor() as cursor:
                cursor.execute("SELECT * FROM qsys.qausrprf ORDER BY username")
                for row in cursor.fetchall():
                    users.append(UserProfile.from_row(row))
        except Exception as e:
            logger.error(f"Failed to list users: {e}")

        return users

    def enable_user(self, username: str) -> tuple[bool, str]:
        """Enable a user profile."""
        self._ensure_initialized()

        username = username.upper().strip()

        if not self.get_user(username):
            return False, f"User {username} not found"

        try:
            with get_cursor() as cursor:
                cursor.execute("""
                    UPDATE qsys.qausrprf SET status = '*ENABLED' WHERE username = %s
                """, (username,))

            # Enable PostgreSQL role login
            role_success, role_msg = set_role_enabled(username, True)
            if not role_success:
                logger.warning(f"User {username} enabled but role update failed: {role_msg}")

            return True, f"User {username} enabled"
        except Exception as e:
            logger.error(f"Failed to enable user {username}: {e}")
            return False, f"Failed to enable user: {e}"

    def disable_user(self, username: str) -> tuple[bool, str]:
        """Disable a user profile."""
        self._ensure_initialized()

        username = username.upper().strip()

        if username == 'QSECOFR':
            return False, "Cannot disable QSECOFR"

        if not self.get_user(username):
            return False, f"User {username} not found"

        try:
            with get_cursor() as cursor:
                cursor.execute("""
                    UPDATE qsys.qausrprf SET status = '*DISABLED' WHERE username = %s
                """, (username,))

            # Disable PostgreSQL role login
            role_success, role_msg = set_role_enabled(username, False)
            if not role_success:
                logger.warning(f"User {username} disabled but role update failed: {role_msg}")

            return True, f"User {username} disabled"
        except Exception as e:
            logger.error(f"Failed to disable user {username}: {e}")
            return False, f"Failed to disable user: {e}"


# Global instance
user_manager = UserManager()

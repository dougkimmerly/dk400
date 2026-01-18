"""
DK/400 User Management

AS/400-style user profiles with password authentication.
Uses PBKDF2 for secure password hashing.
"""
import os
import json
import hashlib
import secrets
from datetime import datetime
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field, asdict


# User data file location
DATA_DIR = Path(os.environ.get('DK400_DATA_DIR', '/app/data'))
USERS_FILE = DATA_DIR / 'users.json'


@dataclass
class UserProfile:
    """AS/400-style user profile."""
    username: str
    password_hash: str
    salt: str
    user_class: str = "*USER"  # *SECOFR, *SECADM, *PGMR, *SYSOPR, *USER
    status: str = "*ENABLED"   # *ENABLED, *DISABLED
    description: str = ""
    created: str = ""
    last_signon: str = ""
    signon_attempts: int = 0
    password_expires: str = "*NOMAX"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'UserProfile':
        return cls(**data)


class UserManager:
    """Manages user profiles and authentication."""

    # Password hashing parameters
    HASH_ITERATIONS = 100000
    HASH_ALGORITHM = 'sha256'

    def __init__(self):
        self.users: dict[str, UserProfile] = {}
        self._ensure_data_dir()
        self._load_users()
        self._ensure_default_users()

    def _ensure_data_dir(self):
        """Ensure data directory exists."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def _load_users(self):
        """Load users from JSON file."""
        if USERS_FILE.exists():
            try:
                with open(USERS_FILE, 'r') as f:
                    data = json.load(f)
                    for username, user_data in data.items():
                        self.users[username] = UserProfile.from_dict(user_data)
            except (json.JSONDecodeError, KeyError):
                # Corrupted file, will recreate with defaults
                self.users = {}

    def _save_users(self):
        """Save users to JSON file."""
        data = {username: user.to_dict() for username, user in self.users.items()}
        with open(USERS_FILE, 'w') as f:
            json.dump(data, f, indent=2)

    def _ensure_default_users(self):
        """Ensure default system users exist."""
        # QSECOFR - Security Officer (admin)
        if 'QSECOFR' not in self.users:
            self.create_user(
                username='QSECOFR',
                password='QSECOFR',  # Default password, should be changed
                user_class='*SECOFR',
                description='Security Officer'
            )

        # QSYSOPR - System Operator
        if 'QSYSOPR' not in self.users:
            self.create_user(
                username='QSYSOPR',
                password='QSYSOPR',
                user_class='*SYSOPR',
                description='System Operator'
            )

        # QUSER - Default user
        if 'QUSER' not in self.users:
            self.create_user(
                username='QUSER',
                password='QUSER',
                user_class='*USER',
                description='Default User'
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
        description: str = ""
    ) -> tuple[bool, str]:
        """Create a new user profile."""
        username = username.upper().strip()

        if not username:
            return False, "Username is required"

        if len(username) > 10:
            return False, "Username must be 10 characters or less"

        if username in self.users:
            return False, f"User {username} already exists"

        if not password:
            return False, "Password is required"

        if len(password) < 1:
            return False, "Password must be at least 1 character"

        salt = self._generate_salt()
        password_hash = self._hash_password(password.upper(), salt)

        user = UserProfile(
            username=username,
            password_hash=password_hash,
            salt=salt,
            user_class=user_class,
            description=description,
            created=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )

        self.users[username] = user
        self._save_users()

        return True, f"User {username} created"

    def delete_user(self, username: str) -> tuple[bool, str]:
        """Delete a user profile."""
        username = username.upper().strip()

        if username in ('QSECOFR', 'QSYSOPR', 'QUSER'):
            return False, f"Cannot delete system user {username}"

        if username not in self.users:
            return False, f"User {username} not found"

        del self.users[username]
        self._save_users()

        return True, f"User {username} deleted"

    def change_password(self, username: str, new_password: str) -> tuple[bool, str]:
        """Change a user's password."""
        username = username.upper().strip()

        if username not in self.users:
            return False, f"User {username} not found"

        if not new_password:
            return False, "Password is required"

        user = self.users[username]
        user.salt = self._generate_salt()
        user.password_hash = self._hash_password(new_password.upper(), user.salt)

        self._save_users()

        return True, f"Password changed for {username}"

    def authenticate(self, username: str, password: str) -> tuple[bool, str]:
        """Authenticate a user."""
        username = username.upper().strip()
        password = password.upper() if password else ""

        if not username:
            return False, "Username is required"

        if username not in self.users:
            return False, "User ID or password not valid"

        user = self.users[username]

        if user.status == "*DISABLED":
            return False, f"User profile {username} is disabled"

        # Verify password
        password_hash = self._hash_password(password, user.salt)

        if password_hash != user.password_hash:
            user.signon_attempts += 1
            self._save_users()
            return False, "User ID or password not valid"

        # Successful authentication
        user.signon_attempts = 0
        user.last_signon = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self._save_users()

        return True, f"Sign on successful"

    def get_user(self, username: str) -> Optional[UserProfile]:
        """Get a user profile."""
        username = username.upper().strip()
        return self.users.get(username)

    def list_users(self) -> list[UserProfile]:
        """List all user profiles."""
        return list(self.users.values())

    def enable_user(self, username: str) -> tuple[bool, str]:
        """Enable a user profile."""
        username = username.upper().strip()

        if username not in self.users:
            return False, f"User {username} not found"

        self.users[username].status = "*ENABLED"
        self._save_users()

        return True, f"User {username} enabled"

    def disable_user(self, username: str) -> tuple[bool, str]:
        """Disable a user profile."""
        username = username.upper().strip()

        if username == 'QSECOFR':
            return False, "Cannot disable QSECOFR"

        if username not in self.users:
            return False, f"User {username} not found"

        self.users[username].status = "*DISABLED"
        self._save_users()

        return True, f"User {username} disabled"


# Global instance
user_manager = UserManager()

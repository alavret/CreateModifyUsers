import unittest
from types import SimpleNamespace
from unittest.mock import patch

import add_users


class RemoveEmptyValuesTest(unittest.TestCase):
    def test_remove_empty_values_keeps_false_and_zero(self):
        self.assertEqual(
            add_users.remove_empty_values(
                {
                    "empty_string": "",
                    "none": None,
                    "empty_dict": {},
                    "empty_list": [],
                    "zero": 0,
                    "false": False,
                    "nested": {"empty": "", "value": "ok"},
                    "list": ["", "value", {}, []],
                }
            ),
            {
                "zero": 0,
                "false": False,
                "nested": {"value": "ok"},
                "list": ["value"],
            },
        )

    def test_create_user_payload_does_not_send_empty_optional_csv_fields(self):
        settings = SimpleNamespace(dry_run=False, send_welcome_email=False, write_personal_email_to_contacts=False)
        users = [
            {
                "first": "Ольга",
                "last": "Алёшина",
                "middle": "Владимировна",
                "login": "aleshina_ov",
                "password": "password",
                "password_change_required": "true",
                "position": "",
                "language": "ru",
                "gender": "",
                "birthday": "",
                "is_admin": "",
                "is_enabled": "",
                "work_phone": "",
                "mobile_phone": "",
                "personal_email": "",
                "department": "Минздрав",
                "aliases": [],
            },
            {
                "first": "Иван",
                "last": "Иванов",
                "middle": "",
                "login": "ivanov_i",
                "password": "password",
                "password_change_required": "true",
                "position": "Инженер",
                "language": "ru",
                "gender": "male",
                "birthday": "1990-01-01",
                "is_admin": "",
                "is_enabled": "",
                "work_phone": "",
                "mobile_phone": "",
                "personal_email": "ivan@example.com",
                "department": "1",
                "aliases": [],
            },
            {
                "first": "Пётр",
                "last": "Петров",
                "middle": "",
                "login": "petrov_p",
                "password": "password",
                "password_change_required": "true",
                "position": "",
                "language": "ru",
                "gender": "",
                "birthday": "",
                "is_admin": "",
                "is_enabled": "",
                "work_phone": "",
                "mobile_phone": "",
                "personal_email": "",
                "department": "1",
                "aliases": [],
            },
        ]
        captured_payloads = []

        def fake_create_user_by_api(_settings, payload):
            captured_payloads.append(payload)
            return True, {"id": "123", "uid": "123"}

        with patch.object(add_users, "create_user_by_api", side_effect=fake_create_user_by_api), \
             patch.object(add_users, "generate_deps_hierarchy_from_api", return_value=[]):
            success, added_users = add_users.add_users_from_file_phase_2(settings, users)

        self.assertTrue(success)
        self.assertEqual(added_users[0]["id"], "123")
        payload = captured_payloads[0]
        self.assertNotIn("position", payload)
        self.assertNotIn("gender", payload)
        self.assertNotIn("birthday", payload)
        self.assertNotIn("contacts", payload)
        self.assertNotIn("about", payload)
        self.assertNotIn("isAdmin", payload)
        self.assertNotIn("isEnabled", payload)
        self.assertEqual(payload["nickname"], "aleshina_ov")
        self.assertEqual(payload["language"], "ru")

        payload_with_email = captured_payloads[1]
        self.assertEqual(payload_with_email["about"], '{"personal_email": "ivan@example.com"}')

        next_payload_without_email = captured_payloads[2]
        self.assertNotIn("about", next_payload_without_email)
        self.assertNotIn("position", next_payload_without_email)


if __name__ == "__main__":
    unittest.main()

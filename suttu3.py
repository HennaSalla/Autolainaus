# OLIONMUODOSTIN JA OLETUSARVOT
# =============================

class RasekoMember():
    """docstring for ClassName."""
    def __init__(self, firstname, lastname, role='Student'):
        self.firstname = firstname
        self.lastname = lastname
        self.role = role

def palautaMerkkijono() -> str | int | bool:
    value = 153
    return value


if __name__ == "__main__":
    
    student = RasekoMember('Jonne', 'Jannari')
    print(f'{student.firstname} rooli organisaatiossa on {student.role}')

    teacher = RasekoMember('Mikko', 'Viljanen', 'Teacher')
    print(f'{teacher.firstname} rooli organisaatiossa on {teacher.role}')
    
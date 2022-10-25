"""
    This is module generates random data. It is built to be used in generating sample data in our
    PySpark jobs during development.
"""

# Standard Library Imports <- This is not required but useful to separate out imports
import datetime
import random
from dataclasses import dataclass, field
import randomCoordinates
from faker import Faker
from helper_printer import print_header

# Most random data is generated using the Faker package.
# More fields are available to extend this data generator
# https://faker.readthedocs.io/en/master/providers/baseprovider.html
fake = Faker(use_weighting=True)


def get_record_action() -> str:
    """
    Returns a random string of possible record changes

    Examples:
        delete\n
        update\n
        insert\n
        no_change

    Returns:
        str
    """
    change_type = ("insert",
                   "update",
                   "delete",
                   "no_change",
                   "update",
                   "no_change")
    return random.choice(change_type)


def random_int() -> int:
    """
    Returns a random integer

    Examples:
        6753

    Returns:
        int
    """
    return fake.pyint()


def random_float() -> float:
    """
    Returns a random float

    Examples:
        39.4500250260523
        -1056327468048.49

    Returns:
        float
    """
    return fake.pyfloat()


def random_bool() -> bool:
    """
    Returns a random bool

    Examples:
        True

    Returns:
        bool
    """
    return fake.pybool()


@dataclass
class RandomData:
    """
    Random data generated for different types of data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        ipv4_address (str): 185.236.185.101\n
        ipv6_address (str): 63c1:1f6b:4d00:24ce:7c93:62c9:4bbc:a34b\n
        lat_lon (tuple): (-53.36, 137.28)\n
        record_action (str): insert\n
        integer_val (int): 7797\n
        float_val (float): 97.3444301554033\n
        bool_val (bool): False

    Attributes:
        ipv4_address (str): Random IPv4 address or network with a valid CIDR\n
        ipv6_address (str): Random IPv6 address or network with a valid CIDR\n
        lat_lon (tuple): Random latitude and longitude coordinate\n
        record_action (str): Random record change value used in ETL\n
        integer_val (int): Random integer\n
        float_val (float): Random float\n
        bool_val (bool): Random bool
    """
    ipv4_address: str = field(default_factory=fake.ipv4)
    ipv6_address: str = field(default_factory=fake.ipv6)
    lat_lon: tuple = field(default_factory=randomCoordinates.DD)
    record_action: str = field(default_factory=get_record_action)
    integer_val: int = field(default_factory=random_int)
    float_val: float = field(default_factory=random_float)
    bool_val: bool = field(default_factory=random_bool)

    def generate(self, identity_id: int = 0):
        """
        Random data generated for different types of data. This has no connection to real data.
        Any representation of real world data is merely a coincidence.

        Examples:
            id (int): 0
            ipv4_address (str): 185.236.185.101
            ipv6_address (str): 63c1:1f6b:4d00:24ce:7c93:62c9:4bbc:a34b
            lat_lon (tuple): (-53.36, 137.28)
            record_action (str): insert
            integer_val (int): 7797
            float_val (float): 97.3444301554033
            bool_val (bool): False

        Args:
            identity_id (int): The identity for this record if you want to supply. Defaults to 0

        Returns:
            A dictionary of all attributes for the class and an identity number
        """
        return dict(
            {
                "id": identity_id,
                "ipv4_address": self.ipv4_address,
                "ipv6_address": self.ipv6_address,
                "lat_lon": self.lat_lon,
                "record_action": self.record_action,
                "integer_val": self.integer_val,
                "float_val": self.float_val,
                "bool_val": self.bool_val
            }
        )


@dataclass
class Address:
    """
    Random data generated for address data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        building_number (str): 389\n
        city (str): Brandon port\n
        city_suffix (str): fort\n
        country (str): Lesotho\n
        country_code (str): FI\n
        current_country (str): United States\n
        postcode (str): 27524\n
        street_address (str): 54156 Sanders Vista Suite 910\n
        street_name (str): Emily Haven\n
        street_suffix (str): Hills\n

    Attributes:
        building_number (str): Random building number
        city (str):  Random city name
        city_suffix (str):  Random city suffix
        country (str):  Random country
        country_code (str):  Random address
        current_country (str):  Random country
        postcode (str):  Random postal code / zipcode
        street_address (str):  Random street address
        street_name (str):  Random street name
        street_suffix (str):  Random stree suffix
    """
    building_number: str = field(default_factory=fake.building_number)
    city: str = field(default_factory=fake.city)
    city_suffix: str = field(default_factory=fake.city_suffix)
    country: str = field(default_factory=fake.country)
    country_code: str = field(default_factory=fake.country_code)
    current_country: str = field(default_factory=fake.current_country)
    postcode: str = field(default_factory=fake.postcode)
    street_address: str = field(default_factory=fake.street_address)
    street_name: str = field(default_factory=fake.street_name)
    street_suffix: str = field(default_factory=fake.street_suffix)

    def generate(self, identity_id: int = 0):
        """
        Random data generated for address data. This has no connection to real data.
        Any representation of real world data is merely a coincidence.

        Examples:
            id (int): 0\n
            building_number (str): 389\n
            city (str): Brandon port\n
            city_suffix (str): fort\n
            country (str): Lesotho\n
            country_code (str): FI\n
            current_country (str): United States\n
            postcode (str): 27524\n
            street_address (str): 54156 Sanders Vista Suite 910\n
            street_name (str): Emily Haven\n
            street_suffix (str): Hills\n

        Args:
            identity_id (int): The identity for this record if you want to supply. Defaults to 0

        Returns:
            A dictionary of all attributes for the class and an identity number
        """
        return dict(
            {
                "id": identity_id,
                "building_number": self.building_number,
                "city": self.city,
                "city_suffix": self.city_suffix,
                "country": self.country,
                "country_code": self.country_code,
                "current_country": self.current_country,
                "postcode": self.postcode,
                "street_address": self.street_address,
                "street_name": self.street_name,
                "street_suffix": self.street_suffix,
            }
        )


@dataclass
class Person:
    """
    Random data generated for a typical and non-binary person data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        ssn (str): 450-58-0484\n
        prefix (str): Mrs.\n
        name (str): Alyssa Thompson\n
        first_name (str): Jeffrey\n
        last_name (str): Mcfarland\n
        suffix (str): IV\n
        ssn_non_binary (str): 305-53-9644\n
        prefix_non_binary (str): Mx.\n
        name_non_binary (str): John Robinson\n
        first_name_non_binary (str): Ashley\n
        last_name_non_binary (str): Beltran\n
        suffix_non_binary (str): DDS\n
        language_name (str): Xhosa\n

    Attributes:
        ssn (str): Random ssn
        prefix (str): Random name prefix
        name (str): Random full name
        first_name (str): Random first name
        last_name (str): Random last name
        suffix (str): Random name suffix
        ssn_non_binary (str): Random non-binary ssn
        prefix_non_binary (str): Random non-binary name prefix
        name_non_binary (str): Random full non-binary name
        first_name_non_binary (str): Random non-binary first name
        last_name_non_binary (str): Random non-binary last name
        suffix_non_binary (str): Random non-binary name suffix
        language_name (str): Random language_name
    """
    ssn: str = field(default_factory=fake.ssn)
    prefix: str = field(default_factory=fake.prefix)
    name: str = field(default_factory=fake.name)
    first_name: str = field(default_factory=fake.first_name)
    last_name: str = field(default_factory=fake.last_name)
    suffix: str = field(default_factory=fake.suffix)
    ssn_non_binary: str = field(default_factory=fake.ssn)
    prefix_non_binary: str = field(default_factory=fake.prefix_nonbinary)
    name_non_binary: str = field(default_factory=fake.name_nonbinary)
    first_name_non_binary: str = field(default_factory=fake.first_name_nonbinary)
    last_name_non_binary: str = field(default_factory=fake.last_name_nonbinary)
    suffix_non_binary: str = field(default_factory=fake.suffix_nonbinary)
    language_name: str = field(default_factory=fake.language_name)

    def generate(self, identity_id: int = 0):
        """
        Random data generated for a typical and non-binary person data. This has no connection to real data.
        Any representation of real world data is merely a coincidence.

        Examples:
            id (int): 0\n
            ssn (str): 450-58-0484\n
            prefix (str): Mrs.\n
            name (str): Alyssa Thompson\n
            first_name (str): Jeffrey\n
            last_name (str): Mcfarland\n
            suffix (str): IV\n
            ssn_non_binary (str): 305-53-9644\n
            prefix_non_binary (str): Mx.\n
            name_non_binary (str): John Robinson\n
            first_name_non_binary (str): Ashley\n
            last_name_non_binary (str): Beltran\n
            suffix_non_binary (str): DDS\n
            language_name (str): Xhosa\n

        Args:
            identity_id (int): The identity for this record if you want to supply. Defaults to 0

        Returns:
            A dictionary of all attributes for the class and an identity number
        """
        return dict(
            {
                "id": identity_id,
                "ssn": self.ssn,
                "prefix": self.prefix,
                "name": self.name,
                "first_name": self.first_name,
                "last_name": self.last_name,
                "suffix": self.suffix,
                "ssn_non_binary": self.ssn_non_binary,
                "prefix_non_binary": self.prefix_non_binary,
                "name_non_binary": self.name_non_binary,
                "first_name_non_binary": self.first_name_non_binary,
                "last_name_non_binary": self.last_name_non_binary,
                "suffix_non_binary": self.suffix_non_binary,
                "language_name": self.language_name,
            }
        )


@dataclass
class CreditCard:
    """
    Random data generated for credit card data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        credit_card_full (str): VISA 19 digit\\\\nMichele Davis\\\\n4435062887177759304 12/29\\\\nCVC: 906\n
        credit_card_provider (str): VISA 19 digit\n
        name_on_card (str): Michele Davis\n
        credit_card_number (str): 4435062887177759304\n
        credit_card_expire (str): 12/29\n
        credit_card_security_code (str): 906\n

    Attributes:
        credit_card_full (str): Random set of credit card details\n
        credit_card_provider (str): Random credit card provider name\n
        name_on_card (str): Random first last name on card\n
        credit_card_number (str): Random valid credit card number\n
        credit_card_expire (str): Random credit card expiry date\n
        credit_card_security_code (str): Random credit card security code\n
    """
    name_on_card: str = field(default_factory=fake.name)
    credit_card_expire: str = field(default_factory=fake.credit_card_expire)
    credit_card_number: str = field(default_factory=fake.credit_card_number)
    credit_card_provider: str = field(default_factory=fake.credit_card_provider)
    credit_card_security_code: str = field(default_factory=fake.credit_card_security_code)
    credit_card_full: str = field(default_factory=fake.credit_card_full)

    def generate(self, identity_id: int = 0):
        """
        Random data generated for credit card data. This has no connection to real data.
        Any representation of real world data is merely a coincidence.

        Examples:
            id (int): 0\n
            credit_card_full (str): VISA 19 digit\\\\nMichele Davis\\\\n4435062887177759304 12/29\\\\nCVC: 906\n
            credit_card_provider (str): VISA 19 digit\n
            name_on_card (str): Michele Davis\n
            credit_card_number (str): 4435062887177759304\n
            credit_card_expire (str): 12/29\n
            credit_card_security_code (str): 906\n

        Args:
            identity_id (int): The identity for this record if you want to supply. Defaults to 0

        Returns:
            A dictionary of all attributes for the class and an identity number
        """
        credit_car_full: list = self.credit_card_full.splitlines()
        credit_car_provider = credit_car_full[0].strip()
        credit_car_name = credit_car_full[1].strip()
        credit_car_line_3 = credit_car_full[2].split(sep=" ")
        credit_car_number = credit_car_line_3[0].strip()
        credit_car_expire = credit_car_line_3[1].strip()
        credit_car_security = credit_car_full[3].split(sep=" ")[1].strip()
        return dict(
            {
                "id": identity_id,
                "credit_card_full": self.credit_card_full.replace("\n", r"\n"),
                "credit_card_provider": credit_car_provider,
                "name_on_card": credit_car_name,
                "credit_card_number": credit_car_number,
                "credit_card_expire": credit_car_expire,
                "credit_card_security_code": credit_car_security
            }
        )


@dataclass
class Transaction:
    """
    Random data generated for transaction data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        username (str): kturner\n
        currency (str): EUR\n
        amount (int): 85764\n

    Attributes:
        username (str): Random username
        currency (str): Random value of USD, GBP, EUR
        amount (int): 85764\n
    """
    username: str = field(default_factory=fake.user_name)
    currency: str = field(default_factory=lambda: random.choice(("USD", "GBP", "EUR")))
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def generate(self, identity_id: int = 0):
        """
        Random data generated for transaction. This has no connection to real data.
        Any representation of real world data is merely a coincidence.

        Examples:
            id (int): 0\n
            username (str): kturner\n
            currency (str): EUR\n
            amount (int): 85764\n

        Args:
            identity_id (int): The identity for this record if you want to supply. Defaults to 0

        Returns:
            A dictionary of all attributes for the class and an identity number
        """
        return dict(
            {
                "id": identity_id,
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount
            }
        )


@dataclass
class Profile:
    """
    Random data generated for personal profile data. This has no connection to real data.
    Any representation of real world data is merely a coincidence.

    Examples:
        profile (dict): {'job': 'Set designer',
        'company': 'Maldonado Ltd',
        'ssn': '323-26-1658',
        'residence': '60006 Sharon Stream Apt. 199\\\\nPortsmouth, CA 64969',
        'current_location': (Decimal('-17.6771925'), Decimal('-76.221701')),
        'blood_group': 'A+',
        'website': ['http://jones-hill.com/', 'http://www.ball-sanchez.biz/', 'http://www.pineda.biz/'],
        'username': 'Kturner',
        'name': 'Daniel Johnson',
        'sex': 'M',
        'address': '72789 Patrick Circle Apt. 366\\\\nHutchinson, OR 49906',
        'mail': 'foxdominique@gmail.com',
        'birthdate': datetime.date(2001, 4, 8)}

    Attributes:
        profile (dict): Random personal profile data in a dictionary
    """

    profile: dict = field(default_factory=fake.profile)

    def generate(self, identity_id: int = 0):
        """
        Random data generated for personal profile data. This has no connection to real data.
        Any representation of real world data is merely a coincidence.

        Examples:
            id (int): 0\n
            job (str): Passenger transport manager\n
            company (str): Wright-Gibson\n
            ssn (str): 804-81-1563\n
            residence (str): 849 Nguyen Squares Suite 703\\\\nTownsend, NV 67872\n
            current_location (tuple): (Decimal('21.775429'), Decimal('81.811279'))\n
            blood_group (str): B-\n
            website (str): https://www.sandoval.net/\n
            username (str): Kturner\n
            name (str): April Walker\n
            sex (str): F\n
            address (str): 43245 Christina Highway\\\\nPort Christopher, MD 71437\n
            email (str): autumn17@gmail.com\n
            birthdate (datetime): 1927-02-23 00:00:00\n

        Args:
            identity_id (int): The identity for this record if you want to supply. Defaults to 0

        Returns:
            A dictionary of all attributes for the class and an identity number
        """
        return dict(
            {
                "id": identity_id,
                "job": self.profile.get("job"),
                "company": self.profile.get("company"),
                "ssn": self.profile.get("ssn"),
                "residence": str(self.profile.get("residence")).replace("\n", r"\n"),
                "current_location": self.profile.get("current_location"),
                "blood_group": self.profile.get("blood_group"),
                "website": self.profile.get("website")[0],
                "username": self.profile.get("username"),
                "name": self.profile.get("name"),
                "sex": self.profile.get("sex"),
                "address": str(self.profile.get("address")).replace("\n", r"\n"),
                "email": self.profile.get("mail"),
                "birthdate": datetime.datetime.strptime(str(self.profile.get("birthdate")), "%Y-%m-%d"),
            }
        )


if __name__ == '__main__':
    def print_dict(_k, _v):
        """

        Args:
            _k: key
            _v: value
        """
        print(f"{_k} ({type(_v).__name__}): {_v}\\n")


    print_header("FUNCTIONS")
    functions = {"get_record_action": get_record_action,
                 "random_int": random_int,
                 "random_float": random_float,
                 "random_bool": random_bool}
    for k, v in functions.items():
        print_dict(k, v())
    print()
    print_header("PERSON")
    for k, v in Person().generate().items():
        print_dict(k, v)
    print()
    print_header("ADDRESS")
    x = Address()
    for k, v in Address().generate().items():
        print_dict(k, v)
    print()
    print_header("TRANSACTION")
    for k, v in Transaction().generate().items():
        print_dict(k, v)
    print()
    print_header("CREDIT CARD")
    for k, v in CreditCard().generate().items():
        print_dict(k, v)
    print()
    print_header("RANDOM_DATA")
    for k, v in RandomData().generate().items():
        print_dict(k, v)
    print()
    print_header("PROFILE")
    for k, v in Profile().generate().items():
        print_dict(k, v)
    print()

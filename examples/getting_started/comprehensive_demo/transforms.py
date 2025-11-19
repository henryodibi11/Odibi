from odibi import transform
import pandas as pd

@transform
def calculate_bonus(context, employee_data: str, bonus_multiplier: float = 1000.0):
    """
    Calculates employee bonuses based on performance score.
    
    Logic: Bonus = (Performance Score / 5.0) * Salary * 0.10
    """
    print(f"Processing bonuses for {employee_data}...")
    
    # Get DataFrame from context
    df = context.get(employee_data)
    
    # Apply logic
    df['bonus'] = (df['performance_score'] / 5.0) * df['salary'] * 0.10
    
    # Add a 'status' column just for fun
    df['status'] = df['performance_score'].apply(lambda x: 'Star' if x >= 4.5 else 'Standard')
    
    return df

@transform
def email_generator(context, source_data: str, domain: str = "company.com"):
    """Generates email addresses for employees."""
    df = context.get(source_data)
    
    # Create email: firstname.lastname@domain
    df['email'] = df['name'].str.lower().str.replace(' ', '.') + '@' + domain
    
    return df

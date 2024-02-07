from django import forms
from django.db import connections
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User



class RegisterUserForm(UserCreationForm):
	email = forms.EmailField(widget=forms.EmailInput(attrs={'class':'form-control'}))
	first_name = forms.CharField(max_length=50, widget=forms.TextInput(attrs={'class':'form-control'}))
	last_name = forms.CharField(max_length=50, widget=forms.TextInput(attrs={'class':'form-control'}))
	

	class Meta:
		model = User
		fields = ('username', 'first_name', 'last_name', 'email', 'password1', 'password2')


	def __init__(self, *args, **kwargs):
		super(RegisterUserForm, self).__init__(*args, **kwargs)

		self.fields['username'].widget.attrs['class'] = 'form-control'
		self.fields['password1'].widget.attrs['class'] = 'form-control'
		self.fields['password2'].widget.attrs['class'] = 'form-control'





class IstForm(forms.Form):
    START_DATE = forms.DateField(
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'})
    )
    END_DATE = forms.DateField(
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'})
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['provinces'] = forms.MultipleChoiceField(
                required=True,  # or False if you want the field to be optional
                choices=self.get_provinces_choices(),
                widget=forms.SelectMultiple(attrs={'class': 'form-control'})
            )

    def get_provinces_choices(self):
        choices = [
            ('','Select a Provinces'),
            ('MashonalandEast', 'Mashonaland East'),
            ('MashonalandWest', 'Mashonaland West'),
            ('Masvingo', 'Masvingo'),
            ('MetebelelandNorth', 'Metebeleland North'),
            ('MetebelelandSouth', 'Metebeleland South'),
            ('MashonalandCentral', 'Mashonaland Central'),
            ('Midlands', 'Midlands'),
            ('Harare', 'Harare'),
            ('Manicaland', 'Manicaland'),
            ('Bulawayo', 'Bulawayo'),
            ('MashanalandEast', 'Mashanaland East')
        ]
        return choices



class RtcqiForm(forms.Form):
    START_DATE = forms.DateField(
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'})
    )
    END_DATE = forms.DateField(
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'})
    )


class WeeklyForm(forms.Form):
    upload_file = forms.FileField(
        label='File Upload',
        widget=forms.ClearableFileInput(attrs={'class': 'form-control' ,id:'formFile'}),
    )



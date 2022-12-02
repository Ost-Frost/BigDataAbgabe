import { NgModule } from '@angular/core';
import { InputTextModule } from 'primeng/inputtext';
import { InputNumberModule } from 'primeng/inputnumber';
import { PasswordModule } from 'primeng/password';
import { ButtonModule } from 'primeng/button';
import { TabMenuModule } from 'primeng/tabmenu';
import { SidebarModule } from 'primeng/sidebar';
import { MenuModule } from 'primeng/menu';
import { ColorPickerModule } from 'primeng/colorpicker';
import { ToastModule } from 'primeng/toast';
import { TableModule } from 'primeng/table';
import { DropdownModule } from 'primeng/dropdown';
import { ProgressSpinnerModule } from 'primeng/progressspinner';
import { FileUploadModule } from 'primeng/fileupload';
import { CheckboxModule } from 'primeng/checkbox';
import { DialogModule } from 'primeng/dialog';
import { ListboxModule } from 'primeng/listbox';
import { InputTextareaModule } from 'primeng/inputtextarea';
import { PaginatorModule } from 'primeng/paginator';
import { AutoCompleteModule } from 'primeng/autocomplete';
import { DividerModule } from 'primeng/divider';
import { CardModule } from 'primeng/card';
import { MultiSelectModule } from 'primeng/multiselect';
import { TooltipModule } from 'primeng/tooltip';


@NgModule({
    exports: [
        InputTextModule,
        ButtonModule,
        TabMenuModule,
        PasswordModule,
        SidebarModule,
        MenuModule,
        ColorPickerModule,
        ToastModule,
        TableModule,
        DropdownModule,
        ProgressSpinnerModule,
        FileUploadModule,
        InputNumberModule,
        CheckboxModule,
        DialogModule,
        ListboxModule,
        InputTextareaModule,
        PaginatorModule,
        AutoCompleteModule,
        DividerModule,
        CardModule,
        MultiSelectModule,
        TooltipModule
    ]
})
export class PrimengModule { }